import os
import sys
import logging
from typing import Dict, Tuple, List, Set, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

TMDB_TOKEN = os.getenv("TMDB_BEARER_TOKEN")
DB_DSN = os.getenv("DB_DSN")

START_YEAR, END_YEAR = 2018, 2025
ACTOR_LIMIT = 5

REQUEST_TIMEOUT = 15  # 秒
PAGE_LIMIT_PER_YEAR: Optional[int] = None
LOG_EVERY = 50

SESSION = requests.Session()


def fatal(msg):
    logging.error(msg)
    sys.exit(1)


def ensure_sequences(conn):
    targets = [
        ("movies", "movieid", "movies_movieid_seq"),
        ("people", "peopleid", "people_peopleid_seq"),
        ("alt_titles", "titleid", "alt_titles_titleid_seq"),
    ]
    with conn.cursor() as cur:
        for table, col, seq in targets:
            cur.execute("SELECT 1 FROM information_schema.sequences WHERE sequence_name = %s", (seq,))
            if cur.fetchone() is None:
                cur.execute(f"CREATE SEQUENCE {seq};")
            cur.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table};")
            max_id = cur.fetchone()[0] or 0
            cur.execute("SELECT setval(%s, %s, false);", (seq, max_id + 1))
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT nextval(%s);", (seq,))
    conn.commit()


def fetch_existing(conn):
    movies_key: Dict[Tuple[str, int, str], int] = {}
    people_key: Dict[Tuple[str, str], int] = {}
    countries: Set[str] = set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT movieid, LOWER(title), year_released, country FROM movies WHERE year_released BETWEEN %s AND %s",
            (START_YEAR, END_YEAR),
        )
        for mid, title, yr, country in cur.fetchall():
            movies_key[(title, yr, country)] = mid

        cur.execute("SELECT peopleid, first_name, surname FROM people")
        for pid, fn, sn in cur.fetchall():
            people_key[(fn or "", sn)] = pid

        cur.execute("SELECT country_code FROM countries")
        for (cc,) in cur.fetchall():
            countries.add(cc.strip().lower())
    return movies_key, people_key, countries


def tmdb_get(url, params=None):
    resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def tmdb_discover(year: int):
    page, total_pages = 1, 1
    while page <= total_pages:
        if PAGE_LIMIT_PER_YEAR is not None and page > PAGE_LIMIT_PER_YEAR:
            break
        data = tmdb_get(
            "https://api.themoviedb.org/3/discover/movie",
            params={
                "primary_release_date.gte": f"{year}-01-01",
                "primary_release_date.lte": f"{year}-12-31",
                "include_adult": False,
                "page": page,
                "sort_by": "popularity.desc",
                "language": "en-US",
            },
        )
        total_pages = data.get("total_pages", 1)
        for item in data.get("results", []):
            yield item
        page += 1


def tmdb_movie_detail(movie_id: int):
    return tmdb_get(f"https://api.themoviedb.org/3/movie/{movie_id}", params={"language": "en-US"})


def tmdb_alt_titles(movie_id: int):
    return tmdb_get(f"https://api.themoviedb.org/3/movie/{movie_id}/alternative_titles").get("titles", [])


def tmdb_credits(movie_id: int):
    return tmdb_get(f"https://api.themoviedb.org/3/movie/{movie_id}/credits", params={"language": "en-US"})


def tmdb_person_detail(person_id: int):
    return tmdb_get(f"https://api.themoviedb.org/3/person/{person_id}")


def norm_title(t: str) -> str:
    return (t or "").strip().lower()


def gender_map(g: int) -> str:
    if g == 1:
        return "F"
    if g == 2:
        return "M"
    return "?"


def split_name(full_name: str):
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


def year_or_none(date_str: str):
    if not date_str:
        return None
    try:
        return int(date_str.split("-")[0])
    except Exception:
        return None


def derive_country_from_detail(detail: dict) -> str:
    spoken = detail.get("spoken_languages") or []
    if spoken:
        code = spoken[0].get("iso_639_1")
        if code:
            return code.lower()
    ol = detail.get("original_language")
    if ol:
        return ol.lower()
    return ""


def insert_movie(cur, movie):
    cur.execute(
        """
        INSERT INTO movies (title, country, year_released, runtime)
        VALUES (%s, %s, %s, %s)
        RETURNING movieid;
        """,
        (movie["title"], movie["country"], movie["year_released"], movie["runtime"]),
    )
    return cur.fetchone()[0]


def upsert_people(cur, person):
    cur.execute(
        """
        INSERT INTO people (first_name, surname, born, died, gender)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (surname, first_name) DO UPDATE
            SET gender = EXCLUDED.gender
        RETURNING peopleid;
        """,
        (
            person["first_name"],
            person["surname"],
            person["born"],
            person["died"],
            person["gender"],
        ),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT peopleid FROM people WHERE surname=%s AND first_name=%s LIMIT 1",
        (person["surname"], person["first_name"]),
    )
    r = cur.fetchone()
    return r[0] if r else None


def upsert_alt_titles(cur, movieid: int, titles: List[str]):
    dedup = {t for t in titles if t}
    if not dedup:
        return
    execute_values(
        cur,
        """
        INSERT INTO alt_titles (movieid, title)
        VALUES %s
        ON CONFLICT (movieid, title) DO NOTHING
        """,
        [(movieid, t) for t in dedup],
    )


def upsert_credits(cur, rows: List[Tuple[int, int, str]]):
    if not rows:
        return
    execute_values(
        cur,
        """
        INSERT INTO credits (movieid, peopleid, credited_as)
        VALUES %s
        ON CONFLICT (movieid, peopleid, credited_as) DO NOTHING
        """,
        rows,
    )


def main():
    if not TMDB_TOKEN:
        fatal("环境变量 TMDB_BEARER_TOKEN 未设置")
    if not DB_DSN:
        fatal("环境变量 DB_DSN 未设置")

    SESSION.headers.update({"accept": "application/json", "Authorization": f"Bearer {TMDB_TOKEN}"})

    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    ensure_sequences(conn)

    movies_key, people_key, countries = fetch_existing(conn)
    logging.info("已有影片 %d 条（2018-2025），人员 %d 条，国家 %d 个", len(movies_key), len(people_key), len(countries))

    new_movies = new_people = new_credits = 0

    try:
        with conn.cursor() as cur:
            for year in range(START_YEAR, END_YEAR + 1):
                seen = inserted = skipped_country = skipped_year = 0
                logging.info("处理年份 %s", year)
                for idx, item in enumerate(tmdb_discover(year), start=1):
                    tmdb_id = item["id"]
                    seen += 1
                    if idx % LOG_EVERY == 0:
                        logging.info("年份 %s 已拉取 %d 条（累计插入 %d，跳过国家 %d）", year, seen, inserted, skipped_country)

                    detail = tmdb_movie_detail(tmdb_id)
                    release_year = year_or_none(detail.get("release_date"))
                    if not release_year or release_year < START_YEAR or release_year > END_YEAR:
                        skipped_year += 1
                        continue

                    main_country = derive_country_from_detail(detail)
                    if not main_country or main_country not in countries:
                        skipped_country += 1
                        continue

                    title = detail.get("title") or detail.get("original_title") or ""
                    key = (norm_title(title), release_year, main_country)
                    if key in movies_key:
                        continue

                    runtime = detail.get("runtime")
                    movieid = insert_movie(
                        cur,
                        {
                            "title": title,
                            "country": main_country,
                            "year_released": release_year,
                            "runtime": runtime,
                        },
                    )
                    movies_key[key] = movieid
                    inserted += 1
                    new_movies += 1

                    alt_titles_resp = tmdb_alt_titles(tmdb_id)
                    alt_list = [t.get("title") for t in alt_titles_resp] + [title, detail.get("original_title")]
                    upsert_alt_titles(cur, movieid, alt_list)

                    credits = tmdb_credits(tmdb_id)
                    credit_rows = []

                    # 演员
                    for cast in credits.get("cast", [])[:ACTOR_LIMIT]:
                        pid = cast["id"]
                        full_name = cast.get("name") or cast.get("original_name") or ""
                        gender = gender_map(cast.get("gender"))
                        fn, sn = split_name(full_name)
                        p_detail = tmdb_person_detail(pid)
                        born = year_or_none(p_detail.get("birthday"))
                        died = year_or_none(p_detail.get("deathday"))

                        p_key = (fn or "", sn)
                        if p_key in people_key:
                            peopleid = people_key[p_key]
                        else:
                            peopleid = upsert_people(
                                cur,
                                {
                                    "first_name": fn,
                                    "surname": sn,
                                    "born": born if born is not None else -1,
                                    "died": died,
                                    "gender": gender,
                                },
                            )
                            people_key[p_key] = peopleid
                            new_people += 1
                        credit_rows.append((movieid, peopleid, "A"))

                    # 导演
                    for crew in credits.get("crew", []):
                        if crew.get("job") != "Director":
                            continue
                        pid = crew["id"]
                        full_name = crew.get("name") or crew.get("original_name") or ""
                        gender = gender_map(crew.get("gender"))
                        fn, sn = split_name(full_name)
                        p_detail = tmdb_person_detail(pid)
                        born = year_or_none(p_detail.get("birthday"))
                        died = year_or_none(p_detail.get("deathday"))

                        p_key = (fn or "", sn)
                        if p_key in people_key:
                            peopleid = people_key[p_key]
                        else:
                            peopleid = upsert_people(
                                cur,
                                {
                                    "first_name": fn,
                                    "surname": sn,
                                    "born": born if born is not None else -1,
                                    "died": died,
                                    "gender": gender,
                                },
                            )
                            people_key[p_key] = peopleid
                            new_people += 1
                        credit_rows.append((movieid, peopleid, "D"))

                    upsert_credits(cur, credit_rows)
                    new_credits += len(credit_rows)

                logging.info(
                    "年份 %s 完成：拉取 %d，插入 %d，跳过国家 %d，跳过年份/无日期 %d",
                    year,
                    seen,
                    inserted,
                    skipped_country,
                    skipped_year,
                )

            conn.commit()
            logging.info("完成：新增影片 %d，新增人员 %d，新增关联 %d", new_movies, new_people, new_credits)
    except Exception:
        conn.rollback()
        logging.exception("发生错误，已回滚")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
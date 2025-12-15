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
REQUEST_TIMEOUT = 15
LOG_EVERY = 50
PAGE_LIMIT_PER_YEAR: Optional[int] = None

# 按季度分片；如需更细可改为月度
RANGES = [("01-01", "03-31"), ("04-01", "06-30"), ("07-01", "09-30"), ("10-01", "12-31")]

SESSION = requests.Session()


def fatal(msg):
    logging.error(msg)
    sys.exit(1)


def trim(s: str, limit: int) -> str:
    if s is None:
        return ""
    s = s.strip()
    return s[:limit]


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
    for start, end in RANGES:
        page, total_pages = 1, 1
        while page <= total_pages:
            if PAGE_LIMIT_PER_YEAR is not None and page > PAGE_LIMIT_PER_YEAR:
                break
            params = {
                "primary_release_date.gte": f"{year}-{start}",
                "primary_release_date.lte": f"{year}-{end}",
                "include_adult": False,
                "include_video": False,
                "page": page,
                "sort_by": "popularity.desc",
                "language": "en-US",
                "with_release_type": "3",  # 院线正式
                "vote_count.gte": 2,
                "with_runtime.gte": 60,
            }
            data = tmdb_get("https://api.themoviedb.org/3/discover/movie", params=params)
            total_pages = min(data.get("total_pages", 1), 500)  # TMDb 可访问上限 500
            yield data, page, total_pages, start, end
            page += 1


def tmdb_movie_detail_with_append(movie_id: int):
    return tmdb_get(
        f"https://api.themoviedb.org/3/movie/{movie_id}",
        params={
            "language": "en-US",
            "append_to_response": "credits,alternative_titles",
        },
    )


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


def derive_country(detail: dict, countries: Set[str]) -> str:
    # 1) production_countries[0].iso_3166_1
    pcs = detail.get("production_countries") or []
    if pcs:
        code = (pcs[0].get("iso_3166_1") or "").lower()
        if code and code in countries:
            return code
    return ""


def insert_movie(cur, movie):
    cur.execute(
        """
        INSERT INTO movies (title, country, year_released, runtime)
        VALUES (%s, %s, %s, %s)
        RETURNING movieid;
        """,
        (
            trim(movie["title"], 100),  # 片名截断到 100
            movie["country"],
            movie["year_released"],
            movie["runtime"],
        ),
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
            trim(person["first_name"], 30),  # 名截断到 30
            trim(person["surname"], 30),     # 姓截断到 30
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
        (trim(person["surname"], 30), trim(person["first_name"], 30)),
    )
    r = cur.fetchone()
    return r[0] if r else None


def upsert_alt_titles(cur, movieid: int, titles: List[str]):
    dedup = {trim(t, 250) for t in titles if t}  # 别名截断到 250
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
    skip_country_counter: Dict[str, int] = {}
    person_cache: Dict[int, dict] = {}

    try:
        with conn.cursor() as cur:
            for year in range(START_YEAR, END_YEAR + 1):
                seen = inserted = skipped_country = skipped_year = 0
                logging.info("处理年份 %s", year)
                for data, page_no, total_pages, start, end in tmdb_discover(year):
                    results = data.get("results", [])
                    if page_no == 1:
                        logging.info("年份 %s 分片 %s-%s 总页数 ≈ %d", year, start, end, total_pages)
                    for item in results:
                        tmdb_id = item["id"]
                        seen += 1
                        if seen % LOG_EVERY == 0:
                            logging.info(
                                "年份 %s 分片 %s-%s 进度：页 %d/%d，已处理 %d 条（插入 %d，跳过国家 %d）",
                                year, start, end, page_no, total_pages, seen, inserted, skipped_country
                            )

                        detail = tmdb_movie_detail_with_append(tmdb_id)
                        release_year = year_or_none(detail.get("release_date"))
                        if not release_year or release_year < START_YEAR or release_year > END_YEAR:
                            skipped_year += 1
                            continue

                        main_country = derive_country(detail, countries)
                        if not main_country:
                            lang = (detail.get("original_language") or "unknown").lower()
                            skip_country_counter[lang] = skip_country_counter.get(lang, 0) + 1
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

                        # alt titles from appended response
                        alt_titles_resp = (detail.get("alternative_titles") or {}).get("titles", [])
                        alt_list = [t.get("title") for t in alt_titles_resp] + [title, detail.get("original_title")]
                        upsert_alt_titles(cur, movieid, alt_list)

                        credits = detail.get("credits") or {}
                        credit_rows = []

                        def get_person(pid: int):
                            if pid in person_cache:
                                return person_cache[pid]
                            p_detail = tmdb_person_detail(pid)
                            person_cache[pid] = p_detail
                            return p_detail

                        # 演员
                        for cast in credits.get("cast", [])[:ACTOR_LIMIT]:
                            pid = cast["id"]
                            full_name = cast.get("name") or cast.get("original_name") or ""
                            if not full_name.strip():
                                continue  # 无姓名，跳过
                            gender = gender_map(cast.get("gender"))
                            fn, sn = split_name(full_name)
                            fn = trim(fn, 30)
                            sn = trim(sn, 30)
                            if not fn and not sn:
                                continue  # 分拆后仍无名姓，跳过

                            p_detail = get_person(pid)
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

                        # 导演（优先 crew，若 crew 无导演则从 cast 中尝试）
                        directors = [c for c in credits.get("crew", []) if c.get("job") == "Director"]
                        if not directors:
                            directors = [
                                c
                                for c in credits.get("cast", [])
                                if c.get("job") == "Director" or c.get("known_for_department") == "Directing"
                            ]

                        for crew in directors:
                            pid = crew["id"]
                            full_name = crew.get("name") or crew.get("original_name") or ""
                            if not full_name.strip():
                                continue  # 无姓名，跳过
                            gender = gender_map(crew.get("gender"))
                            fn, sn = split_name(full_name)
                            fn = trim(fn, 30)
                            sn = trim(sn, 30)
                            if not fn and not sn:
                                continue  # 分拆后仍无名姓，跳过

                            p_detail = get_person(pid)
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
                    "年份 %s 完成：处理 %d，插入 %d，跳过国家 %d，跳过年份/无日期 %d",
                    year, seen, inserted, skipped_country, skipped_year
                )

            conn.commit()
            logging.info("完成：新增影片 %d，新增人员 %d，新增关联 %d", new_movies, new_people, new_credits)
            if skip_country_counter:
                logging.info("未匹配国家代码的语言/代码统计（请确保 countries 表有对应代码）：")
                for lang, cnt in sorted(skip_country_counter.items(), key=lambda x: x[1], reverse=True):
                    logging.info("  %s -> %d", lang, cnt)
    except Exception:
        conn.rollback()
        logging.exception("发生错误，已回滚")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
"""
SQLite database for persistent ad storage.
"""

import os
import json
import sqlite3
from datetime import datetime
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ad_scraper.db")

# All ad fields in schema order
AD_FIELDS = [
    "id", "source_url", "advertiser_name", "advertiser_page_url",
    "ad_text", "headline", "link_description", "call_to_action",
    "landing_page_url", "reactions_count", "comments_count", "shares_count",
    "total_engagement", "post_date", "is_active", "page_follower_count",
    "ad_format", "video_duration", "video_resolution", "video_orientation",
    "has_captions", "caption_style", "has_background_music", "text_on_screen",
    "hook_text", "video_file_path", "thumbnail_file_path",
    "full_transcript", "timestamped_transcript",
    "hook_duration", "total_word_count", "words_per_minute",
    "cta_timestamp", "number_of_scenes", "avg_scene_duration",
    "first_3_seconds", "first_5_seconds", "last_5_seconds",
    "scrape_status", "error_message", "scraped_at",
]


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL,
            advertiser_name TEXT DEFAULT 'N/A',
            advertiser_page_url TEXT DEFAULT 'N/A',
            ad_text TEXT DEFAULT 'N/A',
            headline TEXT DEFAULT 'N/A',
            link_description TEXT DEFAULT 'N/A',
            call_to_action TEXT DEFAULT 'N/A',
            landing_page_url TEXT DEFAULT 'N/A',
            reactions_count TEXT DEFAULT 'N/A',
            comments_count TEXT DEFAULT 'N/A',
            shares_count TEXT DEFAULT 'N/A',
            total_engagement TEXT DEFAULT 'N/A',
            post_date TEXT DEFAULT 'N/A',
            is_active TEXT DEFAULT 'N/A',
            page_follower_count TEXT DEFAULT 'N/A',
            ad_format TEXT DEFAULT 'Unknown',
            video_duration TEXT DEFAULT 'N/A',
            video_resolution TEXT DEFAULT 'N/A',
            video_orientation TEXT DEFAULT 'N/A',
            has_captions TEXT DEFAULT 'N/A',
            caption_style TEXT DEFAULT 'N/A',
            has_background_music TEXT DEFAULT 'N/A',
            text_on_screen TEXT DEFAULT 'N/A',
            hook_text TEXT DEFAULT 'N/A',
            video_file_path TEXT DEFAULT 'N/A',
            thumbnail_file_path TEXT DEFAULT 'N/A',
            full_transcript TEXT DEFAULT 'N/A',
            timestamped_transcript TEXT DEFAULT 'N/A',
            hook_duration TEXT DEFAULT 'N/A',
            total_word_count TEXT DEFAULT 'N/A',
            words_per_minute TEXT DEFAULT 'N/A',
            cta_timestamp TEXT DEFAULT 'N/A',
            number_of_scenes TEXT DEFAULT 'N/A',
            avg_scene_duration TEXT DEFAULT 'N/A',
            first_3_seconds TEXT DEFAULT 'N/A',
            first_5_seconds TEXT DEFAULT 'N/A',
            last_5_seconds TEXT DEFAULT 'N/A',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT DEFAULT '',
            scraped_at TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT,
            total_urls INTEGER DEFAULT 0,
            successful INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            videos_downloaded INTEGER DEFAULT 0,
            transcripts_generated INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            settings TEXT DEFAULT '{}'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def insert_ad(ad_data: dict) -> int:
    conn = get_db()
    fields = [f for f in AD_FIELDS if f != "id" and f in ad_data]
    placeholders = ", ".join(["?" for _ in fields])
    columns = ", ".join(fields)
    values = [ad_data.get(f, "N/A") for f in fields]

    cursor = conn.execute(
        f"INSERT INTO ads ({columns}) VALUES ({placeholders})",
        values
    )
    conn.commit()
    ad_id = cursor.lastrowid
    conn.close()
    return ad_id


def get_ad(ad_id: int) -> dict:
    conn = get_db()
    row = conn.execute("SELECT * FROM ads WHERE id = ?", [ad_id]).fetchone()
    conn.close()
    if row:
        return dict(row)
    return None


def get_all_ads(search: str = "", sort: str = "scraped_at", order: str = "desc",
                page: int = 1, per_page: int = 50) -> dict:
    conn = get_db()
    offset = (page - 1) * per_page

    # Build WHERE clause
    where = "WHERE scrape_status = 'success'"
    params = []
    if search:
        where += " AND (advertiser_name LIKE ? OR ad_text LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    # Validate sort column
    allowed_sorts = {
        "scraped_at", "advertiser_name", "total_engagement",
        "video_duration", "reactions_count", "comments_count",
    }
    if sort not in allowed_sorts:
        sort = "scraped_at"

    # For engagement sorting, try numeric
    if sort == "total_engagement":
        order_clause = f"CAST({sort} AS INTEGER) {order.upper()}"
    else:
        order_clause = f"{sort} {order.upper()}"

    # Count total
    count_row = conn.execute(f"SELECT COUNT(*) FROM ads {where}", params).fetchone()
    total = count_row[0]

    # Fetch page
    rows = conn.execute(
        f"SELECT * FROM ads {where} ORDER BY {order_clause} LIMIT ? OFFSET ?",
        params + [per_page, offset]
    ).fetchall()

    conn.close()

    return {
        "ads": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": max(1, (total + per_page - 1) // per_page),
    }


def get_stats() -> dict:
    conn = get_db()

    total_ads = conn.execute(
        "SELECT COUNT(*) FROM ads WHERE scrape_status = 'success'"
    ).fetchone()[0]

    total_videos = conn.execute(
        "SELECT COUNT(*) FROM ads WHERE ad_format = 'Video' AND scrape_status = 'success'"
    ).fetchone()[0]

    total_transcripts = conn.execute(
        "SELECT COUNT(*) FROM ads WHERE full_transcript != 'N/A' AND full_transcript != '' AND full_transcript NOT LIKE '%Image Ad%' AND scrape_status = 'success'"
    ).fetchone()[0]

    # Average engagement
    avg_eng_row = conn.execute("""
        SELECT AVG(CAST(total_engagement AS REAL))
        FROM ads
        WHERE scrape_status = 'success'
        AND total_engagement NOT LIKE '%N/A%'
        AND CAST(total_engagement AS INTEGER) > 0
    """).fetchone()
    avg_engagement = int(avg_eng_row[0]) if avg_eng_row[0] else 0

    # Last scrape date
    last_scrape = conn.execute(
        "SELECT MAX(scraped_at) FROM ads"
    ).fetchone()[0]

    conn.close()

    return {
        "total_ads": total_ads,
        "total_videos": total_videos,
        "total_transcripts": total_transcripts,
        "avg_engagement": avg_engagement,
        "last_scrape_date": last_scrape or "Never",
    }


def get_ads_by_ids(ids: list) -> list:
    if not ids:
        return []
    conn = get_db()
    placeholders = ", ".join(["?" for _ in ids])
    rows = conn.execute(
        f"SELECT * FROM ads WHERE id IN ({placeholders}) AND scrape_status = 'success'",
        ids
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", [key]).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key: str, value: str):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        [key, value]
    )
    conn.commit()
    conn.close()


def get_all_settings() -> dict:
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def delete_all_data():
    conn = get_db()
    conn.execute("DELETE FROM ads")
    conn.execute("DELETE FROM scrape_sessions")
    conn.commit()
    conn.close()


def get_storage_info(base_dir: str) -> dict:
    import glob
    def dir_size(path):
        total = 0
        if os.path.exists(path):
            for f in glob.glob(os.path.join(path, "**"), recursive=True):
                if os.path.isfile(f):
                    total += os.path.getsize(f)
        return total

    videos_size = dir_size(os.path.join(base_dir, "videos"))
    thumbnails_size = dir_size(os.path.join(base_dir, "thumbnails"))
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0

    return {
        "videos_mb": round(videos_size / (1024 * 1024), 1),
        "thumbnails_mb": round(thumbnails_size / (1024 * 1024), 1),
        "database_mb": round(db_size / (1024 * 1024), 1),
        "total_mb": round((videos_size + thumbnails_size + db_size) / (1024 * 1024), 1),
    }

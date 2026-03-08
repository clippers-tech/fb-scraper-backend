#!/usr/bin/env python3
"""
FastAPI backend for the Facebook Ad Scraper web UI.
Provides REST endpoints + WebSocket for real-time scrape progress.
Uses Apify's Facebook Ad Library Scraper — no browser or login needed.
"""

import os
import sys
import io
import csv
import json
import time
import asyncio
import zipfile
import threading
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, Response
from pydantic import BaseModel

# Project root is the same directory as this file on Render
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from database import (
    init_db, insert_ad, get_ad, get_all_ads, get_stats,
    get_ads_by_ids, get_setting, set_setting, get_all_settings,
    delete_all_data, get_storage_info, get_db,
)

# ── App Setup ─────────────────────────────────────────────

app = FastAPI(title="Facebook Ad Scraper API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database on startup
init_db()

# Global state for scrape progress
scrape_state = {
    "running": False,
    "progress": [],
    "current_index": 0,
    "total": 0,
    "phase": "idle",
    "completed_ads": [],
    "session_id": None,
}

# WebSocket connections for real-time updates
ws_connections: list[WebSocket] = []


# ── Pydantic Models ───────────────────────────────────────

class ScrapeRequest(BaseModel):
    urls: List[str]
    count: int = 50          # Max ads to fetch per URL
    whisper_model: str = "small"
    delay: int = 1           # Delay between video downloads (seconds)
    min_duration: int = 0
    max_duration: int = 9999
    skip_transcribe: bool = False


class SettingsUpdate(BaseModel):
    apify_api_token: Optional[str] = None
    whisper_model: Optional[str] = None
    delay: Optional[int] = None


class ExportRequest(BaseModel):
    ad_ids: List[int]


# ── WebSocket Broadcast ──────────────────────────────────

async def broadcast(message: dict):
    """Send a message to all connected WebSocket clients."""
    dead = []
    for ws in ws_connections:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_connections.remove(ws)


def sync_broadcast(message: dict):
    """Broadcast from a sync context (scraping thread)."""
    loop = None
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        pass

    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(message), loop)


# ── API Endpoints ─────────────────────────────────────────

@app.get("/api/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/stats")
def dashboard_stats():
    return get_stats()


@app.get("/api/ads")
def list_ads(
    search: str = "",
    sort: str = "scraped_at",
    order: str = "desc",
    page: int = 1,
    per_page: int = 50,
):
    return get_all_ads(search=search, sort=sort, order=order, page=page, per_page=per_page)


@app.get("/api/ads/{ad_id}")
def get_ad_detail(ad_id: int):
    ad = get_ad(ad_id)
    if not ad:
        raise HTTPException(status_code=404, detail="Ad not found")
    return ad


@app.get("/api/video/{ad_id}")
def stream_video(ad_id: int):
    ad = get_ad(ad_id)
    if not ad:
        raise HTTPException(status_code=404, detail="Ad not found")

    video_path = ad.get("video_file_path", "")
    if not video_path or not os.path.exists(video_path):
        raise HTTPException(status_code=404, detail="Video file not found")

    return FileResponse(
        video_path,
        media_type="video/mp4",
        filename=os.path.basename(video_path),
    )


@app.get("/api/thumbnail/{ad_id}")
def serve_thumbnail(ad_id: int):
    ad = get_ad(ad_id)
    if not ad:
        raise HTTPException(status_code=404, detail="Ad not found")

    thumb_path = ad.get("thumbnail_file_path", "")
    if not thumb_path or not os.path.exists(thumb_path):
        raise HTTPException(status_code=404, detail="Thumbnail not found")

    return FileResponse(
        thumb_path,
        media_type="image/png",
        filename=os.path.basename(thumb_path),
    )


# ── Scrape Endpoints ─────────────────────────────────────

def validate_facebook_urls(raw_urls: list) -> list:
    """
    Validate and clean Facebook URLs. Accepts both Ad Library and post URLs.
    Returns only valid facebook.com URLs.
    """
    cleaned = []
    for url in raw_urls:
        url = url.strip()
        if url and "facebook.com" in url:
            cleaned.append(url)
    return cleaned


@app.post("/api/scrape")
async def start_scrape(request: ScrapeRequest):
    if scrape_state["running"]:
        raise HTTPException(status_code=409, detail="A scrape is already running")

    # Validate URLs — accept both Ad Library and individual post URLs
    urls = validate_facebook_urls(request.urls)
    if not urls:
        raise HTTPException(status_code=400, detail="No valid Facebook URLs provided")

    # Reset state
    scrape_state["running"] = True
    scrape_state["progress"] = []
    scrape_state["current_index"] = 0
    scrape_state["total"] = 0  # Will be set once Apify returns results
    scrape_state["phase"] = "starting"
    scrape_state["completed_ads"] = []

    # Run scrape in background thread
    thread = threading.Thread(
        target=run_scrape_job,
        args=(urls, request),
        daemon=True,
    )
    thread.start()

    return {
        "status": "started",
        "total_urls": len(urls),
        "message": f"Fetching ads from {len(urls)} URL(s) via Apify...",
    }


@app.get("/api/scrape/status")
def scrape_status():
    return {
        "running": scrape_state["running"],
        "phase": scrape_state["phase"],
        "current_index": scrape_state["current_index"],
        "total": scrape_state["total"],
        "progress": scrape_state["progress"][-20:],  # Last 20 events
        "completed_ads": scrape_state["completed_ads"],
    }


# ── Export Endpoints ──────────────────────────────────────

@app.post("/api/export/csv")
def export_csv(request: ExportRequest):
    ads = get_ads_by_ids(request.ad_ids)
    if not ads:
        raise HTTPException(status_code=404, detail="No ads found for given IDs")

    output = io.StringIO()
    if ads:
        writer = csv.DictWriter(output, fieldnames=ads[0].keys())
        writer.writeheader()
        for ad in ads:
            writer.writerow(ad)

    content = output.getvalue()
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ad_data.csv"},
    )


@app.post("/api/export/claude")
def export_claude(request: ExportRequest):
    ads = get_ads_by_ids(request.ad_ids)
    if not ads:
        raise HTTPException(status_code=404, detail="No ads found for given IDs")

    text = format_analysis_ready(ads)
    return Response(
        content=text,
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=analysis_ready.txt"},
    )


@app.post("/api/export/videos")
def export_videos_zip(request: ExportRequest):
    ads = get_ads_by_ids(request.ad_ids)
    video_ads = [
        a for a in ads
        if a.get("video_file_path")
        and os.path.exists(a.get("video_file_path", ""))
    ]

    if not video_ads:
        raise HTTPException(status_code=404, detail="No video files found")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for ad in video_ads:
            path = ad["video_file_path"]
            zf.write(path, os.path.basename(path))

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=videos.zip"},
    )


# ── Settings Endpoints ────────────────────────────────────

@app.get("/api/settings")
def get_settings():
    settings = get_all_settings()

    # Also read from .env if settings not in DB yet
    env_path = os.path.join(PROJECT_ROOT, ".env")
    env_data = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    env_data[key.strip()] = val.strip()

    apify_token = settings.get("apify_api_token", "") or env_data.get("APIFY_API_TOKEN", "") or os.getenv("APIFY_API_TOKEN", "")

    return {
        "apify_api_token": apify_token,
        "has_credentials": bool(apify_token),
        "whisper_model": settings.get("whisper_model", "small"),
        "delay": int(settings.get("delay", "1")),
        "storage": get_storage_info(PROJECT_ROOT),
    }


@app.post("/api/settings")
def update_settings(request: SettingsUpdate):
    env_path = os.path.join(PROJECT_ROOT, ".env")

    if request.apify_api_token is not None:
        set_setting("apify_api_token", request.apify_api_token)
    if request.whisper_model is not None:
        set_setting("whisper_model", request.whisper_model)
    if request.delay is not None:
        set_setting("delay", str(request.delay))

    # Also write to .env so the scraper modules can pick it up via load_dotenv
    settings = get_all_settings()
    with open(env_path, "w") as f:
        f.write(f"APIFY_API_TOKEN={settings.get('apify_api_token', '')}\n")
        f.write(f"WHISPER_MODEL={settings.get('whisper_model', 'small')}\n")

    return {"status": "saved"}


@app.delete("/api/data")
def clear_all_data():
    import shutil

    delete_all_data()

    # Clear video and thumbnail directories
    for dirname in ["videos", "thumbnails"]:
        dirpath = os.path.join(PROJECT_ROOT, dirname)
        if os.path.exists(dirpath):
            shutil.rmtree(dirpath)
            os.makedirs(dirpath)

    return {"status": "cleared"}


# ── WebSocket ─────────────────────────────────────────────

@app.websocket("/ws/scrape")
async def websocket_scrape(websocket: WebSocket):
    await websocket.accept()
    ws_connections.append(websocket)
    try:
        # Send current state immediately
        await websocket.send_json({
            "type": "state",
            "running": scrape_state["running"],
            "phase": scrape_state["phase"],
            "current_index": scrape_state["current_index"],
            "total": scrape_state["total"],
        })
        # Keep alive
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                # Send ping
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in ws_connections:
            ws_connections.remove(websocket)


# ── Scrape Job Runner ─────────────────────────────────────

def run_scrape_job(urls: list, request: ScrapeRequest):
    """
    Background thread that runs the Apify-based scraping.

    Flow:
      1. Classify URLs into Ad Library vs individual post URLs
      2. Phase "fetching"  — call appropriate Apify actor(s), wait for results
      3. Phase "processing" — for each returned result, download video (if any),
                              transcribe, and analyze locally
      4. Phase "complete"  — report totals
    """
    global scrape_state

    import asyncio
    main_loop = None
    try:
        main_loop = asyncio.get_event_loop()
    except Exception:
        pass

    def emit(msg: dict):
        scrape_state["progress"].append(msg)
        if main_loop and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast(msg), main_loop)

    try:
        from src.config import ScraperConfig
        from src.scraper import ApifyScraper
        from src.video_processor import VideoProcessor
        from src.transcriber import VideoTranscriber
        from src.video_analyzer import VideoAnalyzer
        from src.logger import ScrapeLogger

        # ── Configure ──────────────────────────────────────────
        config = ScraperConfig()
        settings = get_all_settings()

        # Override with DB-stored settings (prefer DB, fall back to env var)
        token = settings.get("apify_api_token", "") or config.apify_api_token or os.getenv("APIFY_API_TOKEN", "")
        if token:
            config.apify_api_token = token

        config.delay = int(settings.get("delay", str(request.delay)))
        config.whisper_model = settings.get("whisper_model", request.whisper_model)
        config.min_duration = request.min_duration
        config.max_duration = request.max_duration

        logger = ScrapeLogger(config.export_dir)

        # Validate API token
        if not config.apify_api_token:
            emit({
                "type": "error",
                "message": "No Apify API token configured. Go to Settings and add your token.",
            })
            scrape_state["running"] = False
            scrape_state["phase"] = "failed"
            return

        # Initialize components
        scraper = ApifyScraper(config, logger)
        video_proc = VideoProcessor(config, logger)
        analyzer = VideoAnalyzer(logger)
        transcriber = None

        # ── Classify URLs ─────────────────────────────────────
        classified = scraper.classify_urls(urls)
        ad_lib_urls = classified["ad_library"]
        post_urls = classified["posts"]

        emit({
            "type": "phase",
            "phase": "classifying",
            "message": f"Classified {len(urls)} URL(s): {len(ad_lib_urls)} Ad Library, {len(post_urls)} individual post(s)",
        })

        # ── Phase 1: Fetching from Apify ────────────────────────
        scrape_state["phase"] = "fetching"

        # Collect (raw_result, parse_method) tuples
        raw_results = []  # list of (raw_dict, "ad" | "post")

        # Fetch Ad Library results
        if ad_lib_urls:
            emit({
                "type": "phase",
                "phase": "fetching",
                "message": f"Fetching ads from Ad Library for {len(ad_lib_urls)} URL(s)...",
            })
            ad_items = scraper.fetch_ads(ad_lib_urls, count=request.count)
            for item in ad_items:
                raw_results.append((item, "ad"))

        # Fetch individual post results
        if post_urls:
            emit({
                "type": "phase",
                "phase": "fetching",
                "message": f"Fetching {len(post_urls)} individual post(s) via Posts Scraper...",
            })
            post_items = scraper.fetch_posts(post_urls)
            for item in post_items:
                raw_results.append((item, "post"))

        if not raw_results:
            emit({
                "type": "error",
                "message": "Apify returned no results. Check your URLs and API token.",
            })
            scrape_state["running"] = False
            scrape_state["phase"] = "failed"
            scraper.close()
            return

        total_ads = len(raw_results)
        scrape_state["total"] = total_ads
        emit({
            "type": "fetched",
            "total": total_ads,
            "message": f"Apify returned {total_ads} result(s). Processing...",
        })

        # ── Phase 2: Whisper will be loaded lazily on first video ad ──
        whisper_loaded = False

        # ── Phase 3: Process each result ────────────────────────
        scrape_state["phase"] = "processing"
        successful = 0
        failed = 0
        videos = 0
        transcripts = 0

        for i, (raw, result_type) in enumerate(raw_results, 1):
            scrape_state["current_index"] = i

            emit({
                "type": "processing",
                "index": i,
                "total": total_ads,
                "message": f"Processing {'post' if result_type == 'post' else 'ad'} {i} of {total_ads}...",
            })

            # Parse with the appropriate method based on URL type
            if result_type == "post":
                ad_data = scraper.parse_post(raw, i)
            else:
                ad_data = scraper.parse_ad(raw, i)

            if ad_data["scrape_status"] != "success":
                failed += 1
                emit({
                    "type": "ad_failed",
                    "index": i,
                    "url": ad_data.get("source_url", "")[:80],
                    "error": ad_data.get("error_message", "Parse error"),
                })
                # Still save to DB so user can see the failure
                ad_data["scraped_at"] = datetime.now().isoformat()
                insert_ad(ad_data)
                continue

            successful += 1
            advertiser = ad_data.get("advertiser_name", "Unknown")

            emit({
                "type": "ad_success",
                "index": i,
                "advertiser": advertiser,
                "format": ad_data.get("ad_format", "Unknown"),
            })

            # ── Video processing ────────────────────────────────
            if ad_data.get("ad_format") == "Video":
                video_url = ad_data.pop("_video_download_url", "")
                audio_url = ad_data.pop("_audio_download_url", "")
                duration_hint = ad_data.pop("_duration_hint", 0)
                # Remove internal thumbnail URL before saving
                ad_data.pop("_thumbnail_url", None)
                ad_data.pop("_platforms", None)
                ad_data.pop("_spend_range", None)

                emit({
                    "type": "downloading",
                    "index": i,
                    "total": total_ads,
                    "message": f"Downloading video {i} of {total_ads}...",
                })

                video_path = ""
                if video_url and audio_url:
                    # DASH video — separate video + audio streams
                    video_path = video_proc.download_dash_video(video_url, audio_url, i, advertiser)
                elif video_url:
                    video_path = video_proc.download_video(video_url, i, advertiser)

                if video_path:
                    videos += 1
                    ad_data["video_file_path"] = video_path

                    video_info = video_proc.get_video_info(video_path)
                    ad_data["video_duration"] = video_info["duration_str"]
                    ad_data["video_resolution"] = video_info["resolution"]
                    ad_data["video_orientation"] = video_info["orientation"]

                    # Duration filter
                    duration = video_info["duration"]
                    if request.min_duration <= duration <= request.max_duration:
                        # Thumbnail from first video frame
                        thumb_path = video_proc.extract_thumbnail(video_path, i)
                        if thumb_path:
                            ad_data["thumbnail_file_path"] = thumb_path

                        # Lazy-load Whisper on first video (saves RAM until needed)
                        if not request.skip_transcribe and not whisper_loaded:
                            whisper_loaded = True
                            scrape_state["phase"] = "loading_whisper"
                            emit({
                                "type": "phase",
                                "phase": "loading_whisper",
                                "message": f"Loading Whisper model ({config.whisper_model})...",
                            })
                            try:
                                transcriber = VideoTranscriber(config.whisper_model, logger)
                                if not transcriber.load_model():
                                    emit({"type": "warning", "message": "Whisper unavailable — skipping transcription"})
                                    transcriber = None
                            except Exception as we:
                                emit({"type": "warning", "message": f"Whisper failed to load: {str(we)} — skipping transcription"})
                                transcriber = None
                            scrape_state["phase"] = "processing"

                        # Transcription
                        if transcriber:
                            emit({
                                "type": "transcribing",
                                "index": i,
                                "total": total_ads,
                                "message": f"Transcribing video {i} of {total_ads}...",
                            })

                            audio_path = video_proc.extract_audio(video_path)
                            if audio_path:
                                transcript_data = transcriber.transcribe(audio_path)
                                ad_data["full_transcript"] = transcript_data.get("full_transcript", "N/A")
                                ad_data["timestamped_transcript"] = transcript_data.get("timestamped_transcript", "N/A")

                                if transcript_data.get("segments"):
                                    ad_data["hook_text"] = transcript_data["segments"][0]["text"]

                                # Video analysis
                                analysis = analyzer.analyze(video_path, video_info, transcript_data, ad_data)
                                for key, value in analysis.items():
                                    ad_data[key] = value

                                video_proc.cleanup_audio(video_path)
                                transcripts += 1
                else:
                    # Video URL was empty or download failed — log but continue
                    emit({
                        "type": "warning",
                        "index": i,
                        "message": f"Video download failed for ad {i} — saving metadata only",
                    })
            else:
                # Image ad — clean up internal fields before saving
                ad_data.pop("_video_download_url", None)
                ad_data.pop("_audio_download_url", None)
                ad_data.pop("_duration_hint", None)
                ad_data.pop("_thumbnail_url", None)
                ad_data.pop("_platforms", None)
                ad_data.pop("_spend_range", None)

            # Save to database
            ad_data["scraped_at"] = datetime.now().isoformat()
            ad_id = insert_ad(ad_data)
            scrape_state["completed_ads"].append(ad_id)

            # Delay between video downloads to avoid hammering CDN
            if i < total_ads and ad_data.get("ad_format") == "Video":
                time.sleep(request.delay)

        # Cleanup
        scraper.close()

        # ── Phase: Complete ─────────────────────────────────────
        scrape_state["phase"] = "complete"
        scrape_state["running"] = False
        emit({
            "type": "complete",
            "successful": successful,
            "failed": failed,
            "videos": videos,
            "transcripts": transcripts,
            "message": f"Done! {successful} ads saved, {failed} failed, {transcripts} transcribed.",
        })

    except Exception as e:
        scrape_state["phase"] = "error"
        scrape_state["running"] = False
        emit({
            "type": "error",
            "message": f"Scrape failed: {str(e)}",
        })


# ── Helper Functions ──────────────────────────────────────

def format_analysis_ready(ads: list) -> str:
    """Generate the analysis_ready.txt formatted text."""
    total = len(ads)
    lines = []
    lines.append(f"FACEBOOK AD ANALYSIS DATA")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Total Ads: {total}")
    lines.append(f"\n{'=' * 60}\n")

    for i, ad in enumerate(ads, 1):
        lines.append(f"{'=' * 60}")
        lines.append(f"AD {i} of {total}")
        lines.append(f"{'=' * 60}\n")

        lines.append(f"ADVERTISER: {ad.get('advertiser_name', 'N/A')}")
        lines.append(f"PAGE URL: {ad.get('advertiser_page_url', 'N/A')}")
        lines.append(f"LANDING PAGE: {ad.get('landing_page_url', 'N/A')}")
        lines.append(f"CTA BUTTON: {ad.get('call_to_action', 'N/A')}")

        fmt = ad.get("ad_format", "Unknown")
        if fmt == "Video":
            lines.append(f"FORMAT: Video — {ad.get('video_duration', 'N/A')} — {ad.get('video_orientation', 'N/A')}")
        else:
            lines.append(f"FORMAT: {fmt}")

        lines.append(f"ENGAGEMENT: {ad.get('reactions_count', 'N/A')} reactions | {ad.get('comments_count', 'N/A')} comments | {ad.get('shares_count', 'N/A')} shares")
        lines.append(f"POST DATE: {ad.get('post_date', 'N/A')}")
        lines.append(f"ACTIVE: {ad.get('is_active', 'N/A')}")
        lines.append(f"SOURCE: {ad.get('source_url', 'N/A')}")

        lines.append(f"\n--- AD COPY ---")
        lines.append(ad.get("ad_text", "N/A"))

        lines.append(f"\n--- HEADLINE & DESCRIPTION ---")
        lines.append(f"Headline: {ad.get('headline', 'N/A')}")
        lines.append(f"Description: {ad.get('link_description', 'N/A')}")

        if fmt == "Video":
            lines.append(f"\n--- VIDEO TRANSCRIPT (TIMESTAMPED) ---")
            lines.append(ad.get("timestamped_transcript", "N/A"))

            lines.append(f"\n--- VIDEO ANALYSIS ---")
            lines.append(f'Hook (first 3 seconds): "{ad.get("first_3_seconds", "N/A")}"')
            lines.append(f'Hook (first 5 seconds): "{ad.get("first_5_seconds", "N/A")}"')
            lines.append(f'Close (last 5 seconds): "{ad.get("last_5_seconds", "N/A")}"')
            lines.append(f"Speaking pace: {ad.get('words_per_minute', 'N/A')} words per minute")
            lines.append(f"Total words: {ad.get('total_word_count', 'N/A')}")
            lines.append(f"Scene cuts: {ad.get('number_of_scenes', 'N/A')}")
            lines.append(f"Avg scene duration: {ad.get('avg_scene_duration', 'N/A')}")
            lines.append(f"CTA spoken at: {ad.get('cta_timestamp', 'N/A')}")
            cap_line = f"Captions: {ad.get('has_captions', 'N/A')}"
            if ad.get("caption_style", "N/A") != "N/A":
                cap_line += f" — {ad.get('caption_style')}"
            lines.append(cap_line)
            lines.append(f"Background music: {ad.get('has_background_music', 'N/A')}")

        lines.append(f"\n{'=' * 60}\n")

    return "\n".join(lines)


def format_single_ad_claude(ad: dict) -> str:
    """Format a single ad for clipboard copy."""
    return format_analysis_ready([ad])


# ── Single ad Claude export endpoint ──────────────────────

@app.get("/api/ads/{ad_id}/claude")
def get_ad_claude_format(ad_id: int):
    ad = get_ad(ad_id)
    if not ad:
        raise HTTPException(status_code=404, detail="Ad not found")
    return {"text": format_analysis_ready([ad])}


# ── Run Server ────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

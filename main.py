#!/usr/bin/env python3
"""
Facebook Ad Scraper & Video Transcription Tool
===============================================
Scrapes Facebook ad posts and Meta Ad Library entries,
downloads videos, transcribes with timestamps, and exports
structured data for AI analysis.

Usage:
    python main.py [OPTIONS]

Options:
    --headless          Run browser in headless mode (default)
    --visible           Show browser window for debugging
    --delay N           Seconds between page scrapes (default: 3)
    --whisper-model M   Whisper model: tiny, base, small, medium, large-v3 (default: medium)
    --skip-transcribe   Skip video transcription (metadata only)
    --min-duration N    Skip videos shorter than N seconds
    --max-duration N    Skip videos longer than N seconds
    --links FILE        Path to links file (default: links.txt)
"""

import os
import sys
import time
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import ScraperConfig
from src.logger import ScrapeLogger
from src.scraper import FacebookScraper
from src.video_processor import VideoProcessor
from src.transcriber import VideoTranscriber
from src.video_analyzer import VideoAnalyzer
from src.exporter import Exporter


BANNER = """
╔═══════════════════════════════════════════════════════╗
║     Facebook Ad Scraper & Video Transcription Tool    ║
║                                                       ║
║     Scrape · Download · Transcribe · Analyze          ║
╚═══════════════════════════════════════════════════════╝
"""


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Facebook Ad Scraper & Video Transcription Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run browser in headless mode (default)",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Show browser window for debugging",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=3,
        help="Seconds delay between each page scrape (default: 3)",
    )
    parser.add_argument(
        "--whisper-model",
        type=str,
        default="medium",
        choices=["tiny", "base", "small", "medium", "large-v3"],
        help="Whisper model for transcription (default: medium)",
    )
    parser.add_argument(
        "--skip-transcribe",
        action="store_true",
        help="Skip video transcription — scrape metadata only",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=0,
        help="Skip videos shorter than N seconds",
    )
    parser.add_argument(
        "--max-duration",
        type=int,
        default=9999,
        help="Skip videos longer than N seconds",
    )
    parser.add_argument(
        "--links",
        type=str,
        default="links.txt",
        help="Path to file with Facebook URLs, one per line (default: links.txt)",
    )

    return parser.parse_args()


def load_urls(links_file: str, base_dir: str) -> list:
    """Load and validate URLs from links file."""
    filepath = os.path.join(base_dir, links_file)

    if not os.path.exists(filepath):
        return []

    urls = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            # Extract URL from markdown links: [text](url)
            if "](http" in line:
                import re
                match = re.search(r'\((https?://[^)]+)\)', line)
                if match:
                    line = match.group(1)
            # Basic validation
            if "facebook.com" in line or "fb.com" in line:
                urls.append(line)

    return urls


def main():
    print(BANNER)

    args = parse_args()

    # Build configuration
    config = ScraperConfig()
    config.headless = not args.visible
    config.delay = args.delay
    config.whisper_model = args.whisper_model
    config.skip_transcribe = args.skip_transcribe
    config.min_duration = args.min_duration
    config.max_duration = args.max_duration
    config.links_file = args.links

    # Initialize logger
    logger = ScrapeLogger(config.export_dir)

    # Validate configuration
    errors = config.validate()
    if errors:
        print("\n  ✗ Configuration errors:")
        for err in errors:
            print(f"    - {err}")
        print("\n  Fix the errors above and try again.")
        print("  See README.md for setup instructions.\n")
        sys.exit(1)

    # Load URLs
    urls = load_urls(config.links_file, config.base_dir)

    if not urls:
        print(f"\n  ✗ No valid Facebook URLs found in {config.links_file}")
        print(f"\n  Create a file called '{config.links_file}' in the project root")
        print(f"  with one Facebook URL per line. Example:\n")
        print(f"  https://www.facebook.com/100092407819115/posts/738393072584280")
        print(f"  https://www.facebook.com/ads/library/?id=987654321\n")
        sys.exit(1)

    # Confirmation
    print(f"  Found {len(urls)} URLs in {config.links_file}")
    print(f"  Export directory: {config.export_dir}")
    print(f"  Browser mode: {'headless' if config.headless else 'visible'}")
    print(f"  Delay between pages: {config.delay}s")
    if not config.skip_transcribe:
        print(f"  Whisper model: {config.whisper_model}")
    else:
        print(f"  Transcription: SKIPPED")
    if config.min_duration > 0:
        print(f"  Min video duration: {config.min_duration}s")
    if config.max_duration < 9999:
        print(f"  Max video duration: {config.max_duration}s")

    print()
    confirm = input("  Start scraping? (y/n): ").strip().lower()
    if confirm != "y":
        print("\n  Aborted.\n")
        sys.exit(0)

    print()

    # ── Initialize all components ─────────────────────────────

    scraper = FacebookScraper(config, logger)
    video_proc = VideoProcessor(config, logger)
    analyzer = VideoAnalyzer(logger)
    exporter = Exporter(config.export_dir, logger)
    transcriber = None

    if not config.skip_transcribe:
        transcriber = VideoTranscriber(config.whisper_model, logger)

    # ── Phase 1: Start browser and login ──────────────────────

    logger.info("Phase 1: Browser setup and login")
    logger.separator()

    try:
        scraper.start_browser()
        if not scraper.login():
            logger.error("Cannot proceed without Facebook login")
            scraper.close()
            sys.exit(1)
    except Exception as e:
        logger.error(f"Browser startup failed: {str(e)}")
        sys.exit(1)

    # ── Phase 2: Load Whisper model ───────────────────────────

    if transcriber:
        logger.separator()
        logger.info("Phase 2: Loading Whisper model")
        if not transcriber.load_model():
            logger.warning("Whisper unavailable — transcription will be skipped")
            transcriber = None

    # ── Phase 3: Scrape all URLs ──────────────────────────────

    logger.separator()
    logger.info(f"Phase 3: Scraping {len(urls)} URLs")
    logger.separator()

    all_ads = []

    for i, url in enumerate(urls, 1):
        logger.progress(i, len(urls), "Scraping ad", url[:80])

        # Scrape metadata
        ad_data = scraper.scrape_url(url, i)
        all_ads.append(ad_data)

        if ad_data["scrape_status"] != "success":
            logger.error(f"FAILED: {ad_data.get('error_message', 'Unknown error')}")
            logger.separator()
            time.sleep(1)
            continue

        logger.success(f"Metadata scraped: {ad_data.get('advertiser_name', 'Unknown')}")

        # ── Phase 3b: Video processing ────────────────────────

        if ad_data.get("ad_format") == "Video":
            video_url = ad_data.pop("_video_download_url", "")

            # Download video
            logger.progress(i, len(urls), "Downloading video",
                          ad_data.get("advertiser_name", ""))

            video_path = video_proc.download_video(video_url, i, ad_data.get("advertiser_name", "unknown"))

            # Fallback to yt-dlp if direct download fails
            if not video_path:
                logger.info("Direct download failed, trying yt-dlp fallback...")
                video_path = video_proc.download_video_yt_dlp(url, i, ad_data.get("advertiser_name", "unknown"))

            if video_path:
                ad_data["video_file_path"] = video_path

                # Get video info (duration, resolution)
                video_info = video_proc.get_video_info(video_path)
                ad_data["video_duration"] = video_info["duration_str"]
                ad_data["video_resolution"] = video_info["resolution"]
                ad_data["video_orientation"] = video_info["orientation"]

                # Check duration filters
                duration = video_info["duration"]
                if duration < config.min_duration:
                    logger.warning(f"Video too short ({duration:.0f}s < {config.min_duration}s), skipping processing")
                elif duration > config.max_duration:
                    logger.warning(f"Video too long ({duration:.0f}s > {config.max_duration}s), skipping processing")
                else:
                    # Extract thumbnail
                    thumb_path = video_proc.extract_thumbnail(video_path, i)
                    if thumb_path:
                        ad_data["thumbnail_file_path"] = thumb_path

                    # ── Phase 3c: Transcription ───────────────

                    if transcriber:
                        logger.progress(i, len(urls), "Transcribing video",
                                      ad_data.get("advertiser_name", ""))

                        # Extract audio
                        audio_path = video_proc.extract_audio(video_path)

                        if audio_path:
                            # Transcribe
                            transcript_data = transcriber.transcribe(audio_path)

                            ad_data["full_transcript"] = transcript_data.get("full_transcript", "N/A")
                            ad_data["timestamped_transcript"] = transcript_data.get("timestamped_transcript", "N/A")

                            # Hook text from first segment
                            if transcript_data.get("segments"):
                                ad_data["hook_text"] = transcript_data["segments"][0]["text"]

                            # ── Phase 3d: Video analysis ──────

                            logger.info("Analyzing video content...")
                            analysis = analyzer.analyze(
                                video_path, video_info, transcript_data, ad_data
                            )

                            # Merge analysis fields
                            for key, value in analysis.items():
                                ad_data[key] = value

                            # Cleanup temp audio file
                            video_proc.cleanup_audio(video_path)

                            logger.success("Video analysis complete")
                        else:
                            logger.warning("Audio extraction failed — skipping transcription")
            else:
                logger.warning("Video download failed — video fields will be N/A")
                ad_data["video_file_path"] = "N/A — Download failed"
        else:
            # Remove internal field for image ads
            ad_data.pop("_video_download_url", None)

        logger.separator()

        # Delay between pages
        if i < len(urls):
            time.sleep(config.delay)

    # ── Phase 4: Close browser ────────────────────────────────

    scraper.close()

    # ── Phase 5: Export everything ────────────────────────────

    logger.separator()
    logger.info("Phase 5: Exporting results")
    logger.separator()

    exporter.export_all(all_ads, len(urls))

    # ── Final summary ─────────────────────────────────────────

    successful = [a for a in all_ads if a["scrape_status"] == "success"]
    failed = [a for a in all_ads if a["scrape_status"] == "failed"]
    video_count = len([a for a in successful if a.get("ad_format") == "Video"])
    transcribed = len([
        a for a in successful
        if a.get("full_transcript") and a["full_transcript"] not in ("N/A", "N/A — Image Ad", "")
    ])

    summary = [
        f"Successfully scraped: {len(successful)}",
        f"Failed: {len(failed)}",
        f"Videos downloaded: {video_count}",
        f"Transcripts generated: {transcribed}",
    ]
    logger.finalize(summary)

    print(f"\n{'═' * 60}")
    print(f"  DONE")
    print(f"{'═' * 60}")
    print(f"  Scraped: {len(successful)} of {len(urls)}")
    print(f"  Failed: {len(failed)}")
    print(f"  Videos: {video_count}")
    print(f"  Transcripts: {transcribed}")
    print(f"\n  Files saved to: {config.export_dir}/")
    print(f"    - ad_data.csv")
    print(f"    - analysis_ready.txt")
    print(f"    - summary.txt")
    print(f"    - scrape_log.txt")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()

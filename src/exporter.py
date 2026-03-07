"""
Export module: generates CSV, analysis_ready.txt, and summary.txt.
"""

import os
import csv
from datetime import datetime
from collections import Counter
from .logger import ScrapeLogger


# All columns for the CSV export (in order)
CSV_COLUMNS = [
    "ad_number",
    "source_url",
    "advertiser_name",
    "advertiser_page_url",
    "ad_text",
    "headline",
    "link_description",
    "call_to_action",
    "landing_page_url",
    "reactions_count",
    "comments_count",
    "shares_count",
    "total_engagement",
    "post_date",
    "is_active",
    "page_follower_count",
    "ad_format",
    "video_duration",
    "video_resolution",
    "video_orientation",
    "has_captions",
    "caption_style",
    "has_background_music",
    "text_on_screen",
    "hook_text",
    "video_file_path",
    "thumbnail_file_path",
    "full_transcript",
    "timestamped_transcript",
    "hook_duration",
    "total_word_count",
    "words_per_minute",
    "cta_timestamp",
    "number_of_scenes",
    "avg_scene_duration",
    "first_3_seconds",
    "first_5_seconds",
    "last_5_seconds",
    "scrape_status",
    "error_message",
]


class Exporter:
    """Generates all output files from scraped ad data."""

    def __init__(self, export_dir: str, logger: ScrapeLogger):
        self.export_dir = export_dir
        self.logger = logger

    def export_all(self, ads: list, total_urls: int):
        """Generate all export files."""
        self.logger.info("Generating export files...")

        self._export_csv(ads)
        self._export_analysis_ready(ads)
        self._export_summary(ads, total_urls)

        self.logger.success(f"All exports saved to: {self.export_dir}")

    def _export_csv(self, ads: list):
        """Export ad_data.csv with all fields as columns."""
        csv_path = os.path.join(self.export_dir, "ad_data.csv")

        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
                writer.writeheader()

                for i, ad in enumerate(ads, 1):
                    row = ad.copy()
                    row["ad_number"] = i
                    writer.writerow(row)

            self.logger.success(f"CSV exported: ad_data.csv ({len(ads)} rows)")

        except Exception as e:
            self.logger.error(f"CSV export failed: {str(e)}")

    def _export_analysis_ready(self, ads: list):
        """Export analysis_ready.txt formatted for AI analysis."""
        txt_path = os.path.join(self.export_dir, "analysis_ready.txt")
        total = len(ads)

        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"FACEBOOK AD ANALYSIS DATA\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Ads: {total}\n")
                f.write(f"\n{'=' * 60}\n\n")

                for i, ad in enumerate(ads, 1):
                    f.write(f"{'=' * 60}\n")
                    f.write(f"AD {i} of {total}\n")
                    f.write(f"{'=' * 60}\n\n")

                    # Basic info
                    f.write(f"ADVERTISER: {ad.get('advertiser_name', 'N/A')}\n")
                    f.write(f"PAGE URL: {ad.get('advertiser_page_url', 'N/A')}\n")
                    f.write(f"LANDING PAGE: {ad.get('landing_page_url', 'N/A')}\n")
                    f.write(f"CTA BUTTON: {ad.get('call_to_action', 'N/A')}\n")

                    # Format line
                    fmt = ad.get("ad_format", "Unknown")
                    if fmt == "Video":
                        dur = ad.get("video_duration", "N/A")
                        orient = ad.get("video_orientation", "N/A")
                        f.write(f"FORMAT: Video — {dur} — {orient}\n")
                    else:
                        f.write(f"FORMAT: {fmt}\n")

                    # Engagement
                    reactions = ad.get("reactions_count", "N/A")
                    comments = ad.get("comments_count", "N/A")
                    shares = ad.get("shares_count", "N/A")
                    f.write(f"ENGAGEMENT: {reactions} reactions | {comments} comments | {shares} shares\n")

                    # Date and status
                    f.write(f"POST DATE: {ad.get('post_date', 'N/A')}\n")
                    f.write(f"ACTIVE: {ad.get('is_active', 'N/A')}\n")
                    f.write(f"FOLLOWERS: {ad.get('page_follower_count', 'N/A')}\n")
                    f.write(f"SOURCE: {ad.get('source_url', 'N/A')}\n")

                    # Status
                    status = ad.get("scrape_status", "unknown")
                    if status == "failed":
                        f.write(f"STATUS: FAILED — {ad.get('error_message', 'Unknown error')}\n")

                    # Ad copy
                    f.write(f"\n--- AD COPY ---\n")
                    ad_text = ad.get("ad_text", "N/A")
                    f.write(f"{ad_text}\n")

                    # Headline & Description
                    f.write(f"\n--- HEADLINE & DESCRIPTION ---\n")
                    f.write(f"Headline: {ad.get('headline', 'N/A')}\n")
                    f.write(f"Description: {ad.get('link_description', 'N/A')}\n")

                    # Video transcript (only for video ads)
                    if fmt == "Video":
                        ts_transcript = ad.get("timestamped_transcript", "N/A")
                        f.write(f"\n--- VIDEO TRANSCRIPT (TIMESTAMPED) ---\n")
                        f.write(f"{ts_transcript}\n")

                        # Video analysis
                        f.write(f"\n--- VIDEO ANALYSIS ---\n")
                        f.write(f'Hook (first 3 seconds): "{ad.get("first_3_seconds", "N/A")}"\n')
                        f.write(f'Hook (first 5 seconds): "{ad.get("first_5_seconds", "N/A")}"\n')
                        f.write(f'Close (last 5 seconds): "{ad.get("last_5_seconds", "N/A")}"\n')
                        f.write(f"Speaking pace: {ad.get('words_per_minute', 'N/A')} words per minute\n")
                        f.write(f"Total words: {ad.get('total_word_count', 'N/A')}\n")
                        f.write(f"Scene cuts: {ad.get('number_of_scenes', 'N/A')}\n")
                        f.write(f"Avg scene duration: {ad.get('avg_scene_duration', 'N/A')}\n")
                        f.write(f"CTA spoken at: {ad.get('cta_timestamp', 'N/A')}\n")
                        f.write(f"Captions: {ad.get('has_captions', 'N/A')}")
                        if ad.get("caption_style", "N/A") != "N/A":
                            f.write(f" — {ad.get('caption_style')}")
                        f.write(f"\n")
                        f.write(f"Background music: {ad.get('has_background_music', 'N/A')}\n")

                    f.write(f"\n{'=' * 60}\n\n")

            self.logger.success(f"Analysis file exported: analysis_ready.txt")

        except Exception as e:
            self.logger.error(f"Analysis export failed: {str(e)}")

    def _export_summary(self, ads: list, total_urls: int):
        """Export summary.txt with session overview."""
        summary_path = os.path.join(self.export_dir, "summary.txt")

        successful = [a for a in ads if a.get("scrape_status") == "success"]
        failed = [a for a in ads if a.get("scrape_status") == "failed"]
        video_ads = [a for a in successful if a.get("ad_format") == "Video"]
        image_ads = [a for a in successful if a.get("ad_format") != "Video"]

        # Videos with transcripts
        transcribed = [
            a for a in video_ads
            if a.get("full_transcript") and a.get("full_transcript") not in ("N/A", "N/A — Image Ad", "")
        ]

        # Average video duration
        durations = []
        for ad in video_ads:
            try:
                dur_str = ad.get("video_duration", "")
                if dur_str and dur_str != "N/A":
                    parts = dur_str.split(":")
                    if len(parts) == 2:
                        durations.append(int(parts[0]) * 60 + int(parts[1]))
            except:
                pass

        avg_duration = 0
        if durations:
            avg_duration = sum(durations) / len(durations)

        # Average engagement
        engagements = []
        for ad in successful:
            try:
                eng = int(str(ad.get("total_engagement", "0")).replace(",", ""))
                if eng > 0:
                    engagements.append(eng)
            except:
                pass

        avg_engagement = 0
        if engagements:
            avg_engagement = sum(engagements) / len(engagements)

        # Most common CTA
        ctas = [a.get("call_to_action", "N/A") for a in successful if a.get("call_to_action", "N/A") != "N/A"]
        cta_counter = Counter(ctas)
        most_common_cta = "N/A"
        cta_count = 0
        if cta_counter:
            most_common_cta, cta_count = cta_counter.most_common(1)[0]

        # Most common orientation
        orientations = [
            a.get("video_orientation", "N/A") for a in video_ads
            if a.get("video_orientation", "N/A") != "N/A"
        ]
        orient_counter = Counter(orientations)
        most_common_orient = "N/A"
        orient_count = 0
        if orient_counter:
            most_common_orient, orient_count = orient_counter.most_common(1)[0]

        # Average speaking pace
        wpms = []
        for ad in transcribed:
            try:
                wpm = int(ad.get("words_per_minute", "0"))
                if wpm > 0:
                    wpms.append(wpm)
            except:
                pass
        avg_wpm = int(sum(wpms) / len(wpms)) if wpms else 0

        try:
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(f"FACEBOOK AD SCRAPE SUMMARY\n")
                f.write(f"{'=' * 40}\n\n")
                f.write(f"Scrape Date: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"Scrape Time: {datetime.now().strftime('%H:%M:%S')}\n\n")

                f.write(f"Total URLs Provided: {total_urls}\n")
                f.write(f"Successfully Scraped: {len(successful)}\n")
                f.write(f"Failed: {len(failed)}\n")
                f.write(f"Video Ads Found: {len(video_ads)}\n")
                f.write(f"Image Ads Found: {len(image_ads)}\n")
                f.write(f"Videos Downloaded: {len([a for a in video_ads if a.get('video_file_path', 'N/A') not in ('N/A', '', 'N/A — Image Ad')])}\n")
                f.write(f"Transcripts Generated: {len(transcribed)}\n\n")

                if avg_duration > 0:
                    m = int(avg_duration // 60)
                    s = int(avg_duration % 60)
                    f.write(f"Average Video Duration: {m}:{s:02d}\n")

                if avg_engagement > 0:
                    f.write(f"Average Engagement: {int(avg_engagement):,}\n")

                if most_common_cta != "N/A":
                    f.write(f'Most Common CTA: "{most_common_cta}" ({cta_count} of {len(successful)})\n')

                if most_common_orient != "N/A":
                    f.write(f"Most Common Orientation: {most_common_orient} ({orient_count} of {len(video_ads)})\n")

                if avg_wpm > 0:
                    f.write(f"Average Speaking Pace: {avg_wpm} wpm\n")

                # List failed URLs
                if failed:
                    f.write(f"\n{'=' * 40}\n")
                    f.write(f"FAILED URLs:\n")
                    for ad in failed:
                        f.write(f"  - {ad.get('source_url', 'Unknown')}\n")
                        f.write(f"    Reason: {ad.get('error_message', 'Unknown')}\n")

            self.logger.success(f"Summary exported: summary.txt")

        except Exception as e:
            self.logger.error(f"Summary export failed: {str(e)}")

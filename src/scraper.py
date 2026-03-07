"""
Facebook Ad Library Scraper using Apify API.
No browser or login needed — works from any cloud server.
"""

import os
import re
import time
import json
import requests
from html import unescape
from .config import ScraperConfig, AD_DATA_TEMPLATE
from .logger import ScrapeLogger


class ApifyScraper:
    """Fetches Facebook ad data via Apify's Ad Library Scraper API."""

    ACTOR_ID = "curious_coder~facebook-ads-library-scraper"
    BASE_URL = "https://api.apify.com/v2"

    def __init__(self, config: ScraperConfig, logger: ScrapeLogger):
        self.config = config
        self.logger = logger
        self.api_token = config.apify_api_token

    def validate_token(self) -> bool:
        """Check if the Apify API token is configured."""
        if not self.api_token:
            self.logger.error("APIFY_API_TOKEN not configured")
            return False
        return True

    def fetch_ads(self, urls: list, count: int = 50) -> list:
        """
        Send URLs to Apify and return raw ad data.
        Uses async run + polling for reliability (sync can timeout for large jobs).

        Args:
            urls: List of Facebook Ad Library URLs or Page URLs
            count: Max ads to fetch per URL

        Returns:
            List of raw Apify ad result dicts
        """
        if not self.api_token:
            self.logger.error("No Apify API token")
            return []

        # Format URLs for Apify input
        apify_urls = []
        for url in urls:
            url = url.strip()
            if url:
                apify_urls.append({"url": url})

        if not apify_urls:
            return []

        input_data = {
            "urls": apify_urls,
            "count": count,
        }

        self.logger.info(f"Sending {len(apify_urls)} URL(s) to Apify (count={count})...")

        # Start async run
        try:
            run_url = f"{self.BASE_URL}/acts/{self.ACTOR_ID}/runs?token={self.api_token}"
            resp = requests.post(run_url, json=input_data, timeout=30)
            resp.raise_for_status()
            run_data = resp.json().get("data", {})
            run_id = run_data.get("id")

            if not run_id:
                self.logger.error("Failed to start Apify run — no run ID returned")
                return []

            self.logger.info(f"Apify run started: {run_id}")

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "unknown"
            self.logger.error(f"Apify API error (HTTP {status}): {str(e)}")
            if status == 401:
                self.logger.error("Invalid Apify API token — check Settings")
            return []
        except Exception as e:
            self.logger.error(f"Failed to start Apify run: {str(e)}")
            return []

        # Poll for completion
        dataset_id = run_data.get("defaultDatasetId")
        status_url = f"{self.BASE_URL}/actor-runs/{run_id}?token={self.api_token}"

        max_wait = 300  # 5 minutes max
        poll_interval = 5
        elapsed = 0

        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            try:
                status_resp = requests.get(status_url, timeout=15)
                status_resp.raise_for_status()
                status_data = status_resp.json().get("data", {})
                run_status = status_data.get("status", "UNKNOWN")

                if run_status == "SUCCEEDED":
                    dataset_id = status_data.get("defaultDatasetId", dataset_id)
                    self.logger.success(f"Apify run completed in {elapsed}s")
                    break
                elif run_status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    self.logger.error(f"Apify run {run_status}")
                    return []
                else:
                    # Still running
                    if elapsed % 15 == 0:
                        self.logger.info(f"Apify run status: {run_status} ({elapsed}s elapsed)")

            except Exception as e:
                self.logger.warning(f"Status poll error: {str(e)}")

        if elapsed >= max_wait:
            self.logger.error("Apify run timed out after 5 minutes")
            return []

        # Fetch results from dataset
        if not dataset_id:
            self.logger.error("No dataset ID from Apify run")
            return []

        try:
            items_url = f"{self.BASE_URL}/datasets/{dataset_id}/items?token={self.api_token}&format=json"
            items_resp = requests.get(items_url, timeout=60)
            items_resp.raise_for_status()
            items = items_resp.json()

            self.logger.success(f"Fetched {len(items)} ads from Apify")
            return items if isinstance(items, list) else []

        except Exception as e:
            self.logger.error(f"Failed to fetch Apify results: {str(e)}")
            return []

    def parse_ad(self, raw: dict, index: int) -> dict:
        """
        Convert a raw Apify result into our standard ad_data format.
        Every field defaults to 'N/A' — never blank, never crash.
        """
        ad_data = AD_DATA_TEMPLATE.copy()

        try:
            # Source / Library URL
            ad_archive_id = str(raw.get("adArchiveID", raw.get("ad_archive_id", "N/A")))
            if ad_archive_id and ad_archive_id != "N/A":
                ad_data["source_url"] = f"https://www.facebook.com/ads/library/?id={ad_archive_id}"
            else:
                ad_data["source_url"] = raw.get("url", "N/A")

            # Advertiser info
            ad_data["advertiser_name"] = raw.get("pageName", raw.get("page_name", "N/A")) or "N/A"
            page_id = raw.get("pageID", raw.get("page_id", ""))
            if page_id:
                ad_data["advertiser_page_url"] = f"https://www.facebook.com/{page_id}"

            # Snapshot data (the actual ad creative)
            snapshot = raw.get("snapshot", {}) or {}

            # Ad text — strip HTML tags from body_markup
            body = snapshot.get("body_markup", "") or snapshot.get("body", {})
            if isinstance(body, dict):
                body = body.get("text", "") or body.get("markup", {}).get("__html", "")
            if body:
                # Strip HTML tags
                clean_text = re.sub(r'<[^>]+>', '', str(body))
                clean_text = unescape(clean_text).strip()
                ad_data["ad_text"] = clean_text if clean_text else "N/A"

            # Headline & description
            ad_data["headline"] = snapshot.get("title", "N/A") or "N/A"
            ad_data["link_description"] = snapshot.get("caption", snapshot.get("link_description", "N/A")) or "N/A"

            # CTA
            cta = snapshot.get("cta_text", "") or snapshot.get("cta_type", "")
            ad_data["call_to_action"] = cta if cta else "N/A"

            # Landing page
            link_url = snapshot.get("link_url", "") or ""
            ad_data["landing_page_url"] = link_url if link_url else "N/A"

            # Date
            start_date = raw.get("startDate", raw.get("start_date", "")) or ""
            if start_date:
                # Convert timestamp to readable format
                try:
                    if isinstance(start_date, (int, float)):
                        from datetime import datetime
                        ad_data["post_date"] = datetime.fromtimestamp(start_date).strftime("%Y-%m-%d")
                    else:
                        ad_data["post_date"] = str(start_date)[:10]
                except Exception:
                    ad_data["post_date"] = str(start_date)

            # Active status
            is_active = raw.get("isActive", raw.get("is_active", None))
            if is_active is True:
                ad_data["is_active"] = "Yes"
            elif is_active is False:
                ad_data["is_active"] = "No"
            else:
                ad_data["is_active"] = "N/A"

            # Engagement — Apify Ad Library doesn't provide engagement data
            ad_data["reactions_count"] = "N/A — Ad Library"
            ad_data["comments_count"] = "N/A — Ad Library"
            ad_data["shares_count"] = "N/A — Ad Library"
            ad_data["total_engagement"] = "N/A — Ad Library"
            ad_data["page_follower_count"] = "N/A — Ad Library"

            # Detect ad format (video or image)
            videos = snapshot.get("videos", []) or []
            images = snapshot.get("images", []) or []
            # Also check for cards (carousel)
            cards = snapshot.get("cards", []) or []

            has_video = len(videos) > 0
            ad_data["ad_format"] = "Video" if has_video else "Image"

            if has_video:
                # Extract video URL (prefer HD)
                video = videos[0] if videos else {}
                video_url = (
                    video.get("video_hd_url", "")
                    or video.get("video_sd_url", "")
                    or video.get("video_preview_image_url", "")
                )
                ad_data["_video_download_url"] = video_url or ""

                # Thumbnail from video preview
                thumb_url = video.get("video_preview_image_url", "")
                ad_data["_thumbnail_url"] = thumb_url or ""
            else:
                # Image ad — mark video fields as N/A
                video_fields = [
                    "video_duration", "video_resolution", "video_orientation",
                    "has_captions", "caption_style", "has_background_music",
                    "text_on_screen", "hook_text", "video_file_path", "thumbnail_file_path",
                    "full_transcript", "timestamped_transcript", "hook_duration",
                    "total_word_count", "words_per_minute", "cta_timestamp",
                    "number_of_scenes", "avg_scene_duration", "first_3_seconds",
                    "first_5_seconds", "last_5_seconds",
                ]
                for f in video_fields:
                    ad_data[f] = "N/A — Image Ad"

                # Store image URL for thumbnail
                if images:
                    ad_data["_thumbnail_url"] = images[0].get("original_image_url", "") or ""
                elif cards:
                    first_card = cards[0] if cards else {}
                    card_images = first_card.get("images", []) or []
                    if card_images:
                        ad_data["_thumbnail_url"] = card_images[0].get("original_image_url", "") or ""

            # Platform info
            platforms = raw.get("publisherPlatform", raw.get("publisher_platform", []))
            if platforms and isinstance(platforms, list):
                ad_data["_platforms"] = ", ".join(platforms)

            # Spend / impressions if available
            spend = raw.get("spend", {})
            if spend and isinstance(spend, dict):
                lower = spend.get("lower_bound", "")
                upper = spend.get("upper_bound", "")
                if lower or upper:
                    ad_data["_spend_range"] = f"{lower}-{upper}"

            ad_data["scrape_status"] = "success"

        except Exception as e:
            ad_data["scrape_status"] = "failed"
            ad_data["error_message"] = f"Parse error: {str(e)}"

        return ad_data

    def close(self):
        """No cleanup needed for API-based scraper."""
        pass

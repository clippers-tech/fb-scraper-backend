"""
Facebook Scraper using Apify API.
Supports two modes:
  1. Ad Library scraping (search URLs) via curious_coder~facebook-ads-library-scraper
  2. Individual post scraping (post URLs) via apify~facebook-posts-scraper
No browser or login needed — works from any cloud server.
"""

import os
import re
import time
import json
import requests
from html import unescape
from xml.etree import ElementTree
from .config import ScraperConfig, AD_DATA_TEMPLATE
from .logger import ScrapeLogger


class ApifyScraper:
    """Fetches Facebook data via Apify APIs."""

    AD_LIBRARY_ACTOR = "curious_coder~facebook-ads-library-scraper"
    POSTS_ACTOR = "apify~facebook-posts-scraper"
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

    def classify_urls(self, urls: list) -> dict:
        """
        Split URLs into two groups:
          - 'ad_library': URLs containing /ads/library (Ad Library search/page URLs)
          - 'posts': Individual post URLs (facebook.com/XXX/posts/YYY)
        """
        ad_library_urls = []
        post_urls = []

        for url in urls:
            url = url.strip()
            if not url:
                continue
            if "/ads/library" in url:
                ad_library_urls.append(url)
            else:
                post_urls.append(url)

        return {"ad_library": ad_library_urls, "posts": post_urls}

    def _run_actor(self, actor_id: str, input_data: dict, max_wait: int = 300) -> list:
        """
        Start an Apify actor run and poll until complete.
        Returns list of result items.
        """
        if not self.api_token:
            self.logger.error("No Apify API token")
            return []

        self.logger.info(f"Starting Apify actor: {actor_id}")

        # Start async run
        try:
            run_url = f"{self.BASE_URL}/acts/{actor_id}/runs?token={self.api_token}"
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
                    if elapsed % 15 == 0:
                        self.logger.info(f"Apify run status: {run_status} ({elapsed}s elapsed)")

            except Exception as e:
                self.logger.warning(f"Status poll error: {str(e)}")

        if elapsed >= max_wait:
            self.logger.error(f"Apify run timed out after {max_wait // 60} minutes")
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

            self.logger.success(f"Fetched {len(items)} result(s) from Apify")
            return items if isinstance(items, list) else []

        except Exception as e:
            self.logger.error(f"Failed to fetch Apify results: {str(e)}")
            return []

    def fetch_ads(self, urls: list, count: int = 50) -> list:
        """
        Fetch ads from Ad Library URLs.
        Uses the curious_coder Ad Library scraper.
        """
        apify_urls = [{"url": u.strip()} for u in urls if u.strip()]
        if not apify_urls:
            return []

        input_data = {
            "urls": apify_urls,
            "count": max(count, 10),  # Apify minimum is 10
        }

        self.logger.info(f"Sending {len(apify_urls)} Ad Library URL(s) to Apify (count={count})...")
        items = self._run_actor(self.AD_LIBRARY_ACTOR, input_data)

        # Filter out error items
        valid = [i for i in items if "error" not in i]
        errors = [i for i in items if "error" in i]
        for err in errors:
            self.logger.warning(f"Apify error: {err.get('error', 'unknown')}")

        return valid

    def fetch_posts(self, urls: list) -> list:
        """
        Fetch individual Facebook posts.
        Uses the official Apify Facebook Posts Scraper.
        """
        start_urls = [{"url": u.strip()} for u in urls if u.strip()]
        if not start_urls:
            return []

        input_data = {
            "startUrls": start_urls,
            "resultsLimit": len(start_urls),
        }

        self.logger.info(f"Fetching {len(start_urls)} individual post(s) via Apify Posts Scraper...")
        return self._run_actor(self.POSTS_ACTOR, input_data, max_wait=300)

    def parse_ad(self, raw: dict, index: int) -> dict:
        """
        Convert a raw Apify Ad Library result into our standard ad_data format.
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

            # Engagement — Ad Library doesn't provide engagement data
            ad_data["reactions_count"] = "N/A — Ad Library"
            ad_data["comments_count"] = "N/A — Ad Library"
            ad_data["shares_count"] = "N/A — Ad Library"
            ad_data["total_engagement"] = "N/A — Ad Library"
            ad_data["page_follower_count"] = "N/A — Ad Library"

            # Detect ad format (video or image)
            videos = snapshot.get("videos", []) or []
            images = snapshot.get("images", []) or []
            cards = snapshot.get("cards", []) or []

            has_video = len(videos) > 0
            ad_data["ad_format"] = "Video" if has_video else "Image"

            if has_video:
                video = videos[0] if videos else {}
                video_url = (
                    video.get("video_hd_url", "")
                    or video.get("video_sd_url", "")
                    or video.get("video_preview_image_url", "")
                )
                ad_data["_video_download_url"] = video_url or ""
                thumb_url = video.get("video_preview_image_url", "")
                ad_data["_thumbnail_url"] = thumb_url or ""
            else:
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

                if images:
                    ad_data["_thumbnail_url"] = images[0].get("original_image_url", "") or ""
                elif cards:
                    first_card = cards[0] if cards else {}
                    card_images = first_card.get("images", []) or []
                    if card_images:
                        ad_data["_thumbnail_url"] = card_images[0].get("original_image_url", "") or ""

            ad_data["scrape_status"] = "success"

        except Exception as e:
            ad_data["scrape_status"] = "failed"
            ad_data["error_message"] = f"Parse error: {str(e)}"

        return ad_data

    def parse_post(self, raw: dict, index: int) -> dict:
        """
        Convert a raw Apify Facebook Posts Scraper result into our standard ad_data format.
        Every field defaults to 'N/A' — never blank, never crash.
        """
        ad_data = AD_DATA_TEMPLATE.copy()

        try:
            # Source URL
            ad_data["source_url"] = raw.get("url", raw.get("facebookUrl", "N/A")) or "N/A"

            # Advertiser / page info
            user = raw.get("user", {}) or {}
            ad_data["advertiser_name"] = user.get("name", raw.get("pageName", "N/A")) or "N/A"

            page_id = user.get("id", raw.get("facebookId", ""))
            if page_id:
                ad_data["advertiser_page_url"] = f"https://www.facebook.com/{page_id}"

            # Ad text
            ad_data["ad_text"] = raw.get("text", "N/A") or "N/A"

            # Headline (use actionLink title if available)
            action_link = raw.get("actionLink", {}) or {}
            ad_data["headline"] = action_link.get("title", "N/A") or "N/A"
            ad_data["link_description"] = action_link.get("link_display", "N/A") or "N/A"

            # CTA
            ad_data["call_to_action"] = action_link.get("title", "N/A") or "N/A"

            # Landing page
            ad_data["landing_page_url"] = action_link.get("url", raw.get("link", "N/A")) or "N/A"

            # Date
            time_str = raw.get("time", "")
            if time_str:
                ad_data["post_date"] = str(time_str)[:10]
            else:
                ts = raw.get("timestamp", "")
                if ts:
                    try:
                        from datetime import datetime
                        ad_data["post_date"] = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")
                    except Exception:
                        ad_data["post_date"] = str(ts)

            # Active status (posts are always "live" if visible)
            ad_data["is_active"] = "Yes"

            # Engagement metrics
            likes = raw.get("likes", 0) or 0
            comments = raw.get("comments", 0) or 0
            shares = raw.get("shares", 0) or 0

            ad_data["reactions_count"] = str(likes)
            ad_data["comments_count"] = str(comments)
            ad_data["shares_count"] = str(shares)
            ad_data["total_engagement"] = str(likes + comments + shares)
            ad_data["page_follower_count"] = "N/A"

            # Detect ad format
            is_video = raw.get("isVideo", False)
            media = raw.get("media", []) or []

            # Also check media items for video type
            if not is_video and media:
                for m in media:
                    if m.get("__typename") == "Video":
                        is_video = True
                        break

            ad_data["ad_format"] = "Video" if is_video else "Image"

            if is_video:
                video_url = ""
                audio_url = ""
                thumb_url = ""
                duration_ms = 0

                for m in media:
                    if m.get("__typename") == "Video":
                        # Thumbnail
                        thumb_img = m.get("thumbnailImage", {}) or {}
                        thumb_url = thumb_img.get("uri", m.get("thumbnail", "")) or ""

                        # Duration from metadata
                        duration_ms = m.get("playable_duration_in_ms", 0) or 0

                        # Try direct playable URL first
                        video_url = m.get("playable_url", m.get("playable_url_quality_hd", "")) or ""

                        # If no direct URL, extract from DASH manifest
                        if not video_url:
                            legacy = m.get("videoDeliveryLegacyFields", {}) or {}
                            dash_xml = legacy.get("dash_manifest_xml_string", "")
                            if dash_xml:
                                extracted = self._extract_dash_urls(dash_xml)
                                video_url = extracted.get("video_url", "")
                                audio_url = extracted.get("audio_url", "")
                        break

                ad_data["_video_download_url"] = video_url
                ad_data["_audio_download_url"] = audio_url
                ad_data["_thumbnail_url"] = thumb_url
                if duration_ms > 0:
                    secs = duration_ms / 1000
                    ad_data["_duration_hint"] = secs
            else:
                # Image ad — mark video fields
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

                # Get thumbnail from image media
                if media:
                    for m in media:
                        thumb_img = m.get("thumbnailImage", {}) or {}
                        thumb = thumb_img.get("uri", m.get("thumbnail", "")) or ""
                        if thumb:
                            ad_data["_thumbnail_url"] = thumb
                            break

            ad_data["scrape_status"] = "success"

        except Exception as e:
            ad_data["scrape_status"] = "failed"
            ad_data["error_message"] = f"Parse error: {str(e)}"

        return ad_data

    def _extract_dash_urls(self, dash_xml: str) -> dict:
        """
        Parse a DASH manifest XML and extract the best video and audio URLs.
        DASH manifests from Facebook contain separate video/audio streams
        with direct fbcdn.net CDN URLs in <BaseURL> elements.
        """
        result = {"video_url": "", "audio_url": ""}

        try:
            # Unescape HTML entities
            xml_str = unescape(dash_xml)

            # Use regex to extract representations (more robust than XML parsing
            # since the manifest may have namespace issues)
            reps = re.findall(
                r'<Representation([^>]+)>.*?<BaseURL>([^<]+)</BaseURL>',
                xml_str,
                re.DOTALL,
            )

            best_video_bw = 0
            best_audio_bw = 0

            for attrs, base_url in reps:
                base_url = unescape(base_url.strip())

                # Determine bandwidth
                bw_match = re.search(r'bandwidth="(\d+)"', attrs)
                bw = int(bw_match.group(1)) if bw_match else 0

                # Determine codec to distinguish video vs audio
                codecs_match = re.search(r'codecs="([^"]+)"', attrs)
                codecs = codecs_match.group(1) if codecs_match else ""

                is_audio = "mp4a" in codecs.lower() or "audio" in codecs.lower()

                if is_audio:
                    if bw > best_audio_bw:
                        best_audio_bw = bw
                        result["audio_url"] = base_url
                else:
                    # For video, pick a reasonable quality (720p-ish)
                    # to balance download time vs quality on 512MB server
                    width_match = re.search(r'width="(\d+)"', attrs)
                    width = int(width_match.group(1)) if width_match else 0

                    # Prefer ~720p for balance, but take best available up to 720p
                    # If only higher res available, take the lowest one above 720p
                    if width <= 720 and bw > best_video_bw:
                        best_video_bw = bw
                        result["video_url"] = base_url

            # If no video <= 720p found, take the lowest bandwidth video available
            if not result["video_url"]:
                for attrs, base_url in reps:
                    codecs_match = re.search(r'codecs="([^"]+)"', attrs)
                    codecs = codecs_match.group(1) if codecs_match else ""
                    if "mp4a" not in codecs.lower():
                        bw_match = re.search(r'bandwidth="(\d+)"', attrs)
                        bw = int(bw_match.group(1)) if bw_match else 0
                        if not result["video_url"] or bw < best_video_bw:
                            best_video_bw = bw
                            result["video_url"] = unescape(base_url.strip())

            if result["video_url"]:
                self.logger.info("Extracted video URL from DASH manifest")
            if result["audio_url"]:
                self.logger.info("Extracted audio URL from DASH manifest")

        except Exception as e:
            self.logger.warning(f"DASH manifest parse error: {str(e)}")

        return result

    def close(self):
        """No cleanup needed for API-based scraper."""
        pass

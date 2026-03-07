"""
Configuration and constants for the Facebook Ad Scraper.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ScraperConfig:
    """All configuration options for the scraper."""

    # Apify API token
    apify_api_token: str = ""

    # File paths
    links_file: str = "links.txt"
    base_dir: str = ""
    export_dir: str = ""
    videos_dir: str = ""
    thumbnails_dir: str = ""

    # Scraping options
    delay: int = 1
    max_retries: int = 2

    # Video options
    skip_transcribe: bool = False
    whisper_model: str = "small"
    min_duration: int = 0
    max_duration: int = 9999

    def __post_init__(self):
        # Load API token from environment
        self.apify_api_token = os.getenv("APIFY_API_TOKEN", "")

        # Set up directory paths
        if not self.base_dir:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        today = datetime.now().strftime("%Y-%m-%d")
        self.export_dir = os.path.join(self.base_dir, "exports", today)
        self.videos_dir = os.path.join(self.base_dir, "videos")
        self.thumbnails_dir = os.path.join(self.base_dir, "thumbnails")

        # Create all directories
        for d in [self.export_dir, self.videos_dir, self.thumbnails_dir]:
            os.makedirs(d, exist_ok=True)

    def validate(self) -> list:
        """Return list of validation errors, empty if valid."""
        errors = []
        if not self.apify_api_token:
            errors.append("APIFY_API_TOKEN not set — add it in Settings")
        return errors


# Default ad data template with all fields
AD_DATA_TEMPLATE = {
    # Metadata
    "source_url": "N/A",
    "advertiser_name": "N/A",
    "advertiser_page_url": "N/A",
    "ad_text": "N/A",
    "headline": "N/A",
    "link_description": "N/A",
    "call_to_action": "N/A",
    "landing_page_url": "N/A",
    "reactions_count": "N/A",
    "comments_count": "N/A",
    "shares_count": "N/A",
    "total_engagement": "N/A",
    "post_date": "N/A",
    "is_active": "N/A",
    "page_follower_count": "N/A",

    # Video-specific
    "video_duration": "N/A",
    "video_resolution": "N/A",
    "video_orientation": "N/A",
    "has_captions": "N/A",
    "caption_style": "N/A",
    "has_background_music": "N/A",
    "text_on_screen": "N/A",
    "hook_text": "N/A",
    "video_file_path": "N/A",
    "thumbnail_file_path": "N/A",

    # Transcript
    "full_transcript": "N/A",
    "timestamped_transcript": "N/A",

    # Video analysis
    "hook_duration": "N/A",
    "total_word_count": "N/A",
    "words_per_minute": "N/A",
    "cta_timestamp": "N/A",
    "number_of_scenes": "N/A",
    "avg_scene_duration": "N/A",
    "first_3_seconds": "N/A",
    "first_5_seconds": "N/A",
    "last_5_seconds": "N/A",

    # Internal tracking
    "ad_format": "Unknown",
    "scrape_status": "pending",
    "error_message": "",
}

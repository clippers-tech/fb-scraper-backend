"""
Facebook Ad Scraper using Playwright.
Handles both standard post URLs and Meta Ad Library URLs.
"""

import os
import re
import time
import json
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, TimeoutError as PwTimeout
from .config import ScraperConfig, AD_DATA_TEMPLATE
from .logger import ScrapeLogger


class FacebookScraper:
    """Scrapes Facebook ad posts and Ad Library entries."""

    def __init__(self, config: ScraperConfig, logger: ScrapeLogger):
        self.config = config
        self.logger = logger
        self.playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None

    def start_browser(self):
        """Launch Playwright browser with persistent context for login cookies."""
        self.logger.info("Launching browser...")
        self.playwright = sync_playwright().start()

        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir=self.config.browser_data_dir,
            headless=self.config.headless,
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        self.page = self.context.new_page()

        # Stealth: remove webdriver flag
        self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

        self.logger.success("Browser launched")

    def login(self):
        """Log into Facebook if not already logged in."""
        self.logger.info("Checking Facebook login status...")
        self.page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)

        # Check if already logged in by looking for the profile/home elements
        if self._is_logged_in():
            self.logger.success("Already logged in (session restored from cookies)")
            return True

        self.logger.info("Not logged in. Logging in now...")

        if not self.config.fb_email or not self.config.fb_password:
            self.logger.error("No Facebook credentials found in .env file")
            return False

        try:
            # Navigate to login page
            self.page.goto("https://www.facebook.com/login/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            # Fill email
            email_field = self.page.locator('input[name="email"], input#email')
            email_field.fill(self.config.fb_email)
            time.sleep(0.5)

            # Fill password
            pass_field = self.page.locator('input[name="pass"], input#pass')
            pass_field.fill(self.config.fb_password)
            time.sleep(0.5)

            # Click login button
            login_btn = self.page.locator('button[name="login"], button[type="submit"], input[type="submit"]')
            login_btn.first.click()
            time.sleep(5)

            # Check for 2FA or checkpoint
            current_url = self.page.url
            if "checkpoint" in current_url or "two_step_verification" in current_url:
                self.logger.warning("Facebook requires 2FA/checkpoint verification.")
                self.logger.warning("Run with --visible flag to complete verification manually.")
                self.logger.info("Waiting 60 seconds for manual verification...")
                time.sleep(60)

            if self._is_logged_in():
                self.logger.success("Login successful")
                return True
            else:
                self.logger.error("Login failed. Check credentials in .env file.")
                return False

        except Exception as e:
            self.logger.error(f"Login error: {str(e)}")
            return False

    def _is_logged_in(self) -> bool:
        """Check if the user is currently logged into Facebook."""
        try:
            # Multiple checks for logged-in state
            page_content = self.page.content()
            indicators = [
                'aria-label="Home"',
                'aria-label="Your profile"',
                'aria-label="Messenger"',
                'aria-label="Notifications"',
                '/me"',
                'role="banner"',
            ]
            for indicator in indicators:
                if indicator in page_content:
                    return True

            # Check URL - if redirected to login, not logged in
            if "/login" in self.page.url or "login" in self.page.url:
                return False

            # Check for common logged-in elements
            try:
                self.page.locator('[aria-label="Home"]').wait_for(timeout=3000)
                return True
            except:
                pass

            return False
        except:
            return False

    def classify_url(self, url: str) -> str:
        """Determine URL type: 'post', 'ad_library', or 'unknown'."""
        if "/ads/library" in url:
            return "ad_library"
        elif "/posts/" in url or "/videos/" in url or "/photos/" in url or "/reel/" in url:
            return "post"
        elif "facebook.com" in url or "fb.com" in url:
            return "post"  # Try as post
        else:
            return "unknown"

    def scrape_url(self, url: str, index: int) -> dict:
        """
        Scrape a single Facebook URL. Returns ad data dict.
        Retries up to config.max_retries times on failure.
        """
        ad_data = AD_DATA_TEMPLATE.copy()
        ad_data["source_url"] = url

        url_type = self.classify_url(url)
        if url_type == "unknown":
            ad_data["scrape_status"] = "failed"
            ad_data["error_message"] = "Unrecognized URL format"
            return ad_data

        last_error = ""
        for attempt in range(1, self.config.max_retries + 2):
            try:
                if attempt > 1:
                    self.logger.info(f"Retry attempt {attempt - 1} of {self.config.max_retries}...")
                    time.sleep(self.config.delay)

                if url_type == "ad_library":
                    ad_data = self._scrape_ad_library(url, ad_data, index)
                else:
                    ad_data = self._scrape_post(url, ad_data, index)

                if ad_data["scrape_status"] == "success":
                    return ad_data

                last_error = ad_data.get("error_message", "Unknown error")

            except Exception as e:
                last_error = str(e)
                self.logger.error(f"Attempt {attempt} failed: {last_error}")

        # All retries exhausted
        ad_data["scrape_status"] = "failed"
        ad_data["error_message"] = f"All attempts failed. Last error: {last_error}"
        return ad_data

    def _scrape_post(self, url: str, ad_data: dict, index: int) -> dict:
        """Scrape a standard Facebook post URL."""
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(self.config.delay + 2)  # Extra wait for dynamic content

            # Close any popups/overlays
            self._dismiss_popups()

            # Scroll down to load content
            self.page.evaluate("window.scrollTo(0, 300)")
            time.sleep(1)

            # --- ADVERTISER NAME ---
            ad_data["advertiser_name"] = self._extract_advertiser_name()

            # --- ADVERTISER PAGE URL ---
            ad_data["advertiser_page_url"] = self._extract_page_url()

            # --- AD TEXT (with "See More" expansion) ---
            ad_data["ad_text"] = self._extract_ad_text()

            # --- HEADLINE & DESCRIPTION ---
            headline, description = self._extract_headline_description()
            ad_data["headline"] = headline
            ad_data["link_description"] = description

            # --- CTA BUTTON ---
            ad_data["call_to_action"] = self._extract_cta()

            # --- LANDING PAGE URL ---
            ad_data["landing_page_url"] = self._extract_landing_url()

            # --- ENGAGEMENT ---
            reactions, comments, shares = self._extract_engagement()
            ad_data["reactions_count"] = reactions
            ad_data["comments_count"] = comments
            ad_data["shares_count"] = shares
            ad_data["total_engagement"] = self._compute_total_engagement(reactions, comments, shares)

            # --- POST DATE ---
            ad_data["post_date"] = self._extract_post_date()

            # --- PAGE FOLLOWERS ---
            ad_data["page_follower_count"] = self._extract_follower_count()

            # --- DETECT AD FORMAT (Video or Image) ---
            has_video = self._detect_video()
            ad_data["ad_format"] = "Video" if has_video else "Image"

            if not has_video:
                # Mark all video fields as N/A for image ads
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

            # --- VIDEO URL EXTRACTION ---
            if has_video:
                video_url = self._extract_video_url()
                ad_data["_video_download_url"] = video_url

            ad_data["is_active"] = "Yes"  # If we can see it, it's likely active
            ad_data["scrape_status"] = "success"

        except PwTimeout:
            ad_data["scrape_status"] = "retry"
            ad_data["error_message"] = "Page load timeout"
        except Exception as e:
            ad_data["scrape_status"] = "retry"
            ad_data["error_message"] = str(e)

        return ad_data

    def _scrape_ad_library(self, url: str, ad_data: dict, index: int) -> dict:
        """Scrape a Meta Ad Library URL."""
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(self.config.delay + 3)

            # Close any popups
            self._dismiss_popups()

            # Ad Library has a different structure
            page_content = self.page.content()

            # --- ADVERTISER NAME ---
            try:
                # Ad Library shows page name prominently
                name_selectors = [
                    'div[class*="advertiser"] a',
                    'a[href*="facebook.com/"][class*="name"]',
                    'span[class*="x1lliihq"]',
                    'div._7jyg a',
                    'div[class*="_8jg-"] a',
                ]
                for sel in name_selectors:
                    try:
                        el = self.page.locator(sel).first
                        name = el.text_content(timeout=2000)
                        if name and len(name.strip()) > 1:
                            ad_data["advertiser_name"] = name.strip()
                            # Try to get page URL from same element
                            href = el.get_attribute("href")
                            if href:
                                ad_data["advertiser_page_url"] = href
                            break
                    except:
                        continue
            except:
                pass

            # --- AD TEXT ---
            try:
                # Ad Library shows the ad creative text
                text_selectors = [
                    'div._4ik4._4ik5 div',
                    'div[class*="x1iorvi4"]',
                    'div._7jyr',
                    'div[class*="creative"] div[dir="auto"]',
                    'div[data-testid="ad_creative_body"]',
                ]
                for sel in text_selectors:
                    try:
                        els = self.page.locator(sel).all()
                        for el in els:
                            text = el.text_content(timeout=2000)
                            if text and len(text.strip()) > 20:
                                ad_data["ad_text"] = text.strip()
                                break
                        if ad_data["ad_text"] != "N/A":
                            break
                    except:
                        continue
            except:
                pass

            # --- AD LIBRARY SPECIFIC: Status ---
            try:
                status_text = self.page.locator('text=Active').first.text_content(timeout=2000)
                ad_data["is_active"] = "Yes" if "Active" in status_text else "No"
            except:
                try:
                    status_text = self.page.locator('text=Inactive').first.text_content(timeout=2000)
                    ad_data["is_active"] = "No"
                except:
                    ad_data["is_active"] = "N/A"

            # --- STARTED DATE ---
            try:
                date_selectors = [
                    'text=/Started running/',
                    'span:has-text("Started running")',
                ]
                for sel in date_selectors:
                    try:
                        el = self.page.locator(sel).first
                        text = el.text_content(timeout=2000)
                        if text:
                            ad_data["post_date"] = text.strip()
                            break
                    except:
                        continue
            except:
                pass

            # --- VIDEO DETECTION ---
            has_video = self._detect_video()
            ad_data["ad_format"] = "Video" if has_video else "Image"

            if has_video:
                video_url = self._extract_video_url()
                ad_data["_video_download_url"] = video_url

            if not has_video:
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

            # Ad Library typically doesn't show engagement metrics
            ad_data["reactions_count"] = "N/A — Ad Library"
            ad_data["comments_count"] = "N/A — Ad Library"
            ad_data["shares_count"] = "N/A — Ad Library"
            ad_data["total_engagement"] = "N/A — Ad Library"

            ad_data["scrape_status"] = "success"

        except PwTimeout:
            ad_data["scrape_status"] = "retry"
            ad_data["error_message"] = "Page load timeout"
        except Exception as e:
            ad_data["scrape_status"] = "retry"
            ad_data["error_message"] = str(e)

        return ad_data

    # ── Helper extraction methods ─────────────────────────────────

    def _dismiss_popups(self):
        """Close common Facebook popups and overlays."""
        popup_selectors = [
            '[aria-label="Close"]',
            '[aria-label="Decline optional cookies"]',
            'div[role="dialog"] [aria-label="Close"]',
            'button:has-text("Not Now")',
            'button:has-text("Decline")',
            'a:has-text("Not Now")',
        ]
        for sel in popup_selectors:
            try:
                btn = self.page.locator(sel).first
                if btn.is_visible(timeout=1000):
                    btn.click(timeout=2000)
                    time.sleep(0.5)
            except:
                continue

    def _extract_advertiser_name(self) -> str:
        """Extract the page/advertiser name from a post."""
        selectors = [
            'h2 a strong span',
            'h3 a strong span',
            'a[role="link"] strong span',
            'span.x193iq5w strong span',
            'h2 span a strong span',
            'a[aria-label] strong',
            'h4 a',
        ]
        for sel in selectors:
            try:
                el = self.page.locator(sel).first
                text = el.text_content(timeout=2000)
                if text and len(text.strip()) > 1 and len(text.strip()) < 100:
                    return text.strip()
            except:
                continue
        return "N/A"

    def _extract_page_url(self) -> str:
        """Extract the advertiser's Facebook page URL."""
        selectors = [
            'h2 a',
            'h3 a',
            'a[role="link"][tabindex="0"]',
        ]
        for sel in selectors:
            try:
                el = self.page.locator(sel).first
                href = el.get_attribute("href", timeout=2000)
                if href and "facebook.com" in href:
                    # Clean tracking params
                    parsed = urlparse(href)
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    return clean.rstrip("/")
            except:
                continue
        return "N/A"

    def _extract_ad_text(self) -> str:
        """Extract full ad text, clicking 'See more' first if present."""
        # First try to expand "See more"
        see_more_selectors = [
            'div[role="button"]:has-text("See more")',
            'div:has-text("See more")[role="button"]',
            'span:has-text("See more")',
            'div.x1i10hfl:has-text("See more")',
        ]
        for sel in see_more_selectors:
            try:
                btn = self.page.locator(sel).first
                if btn.is_visible(timeout=2000):
                    btn.click(timeout=3000)
                    time.sleep(1)
                    break
            except:
                continue

        # Now extract the text
        text_selectors = [
            'div[data-ad-comet-preview="message"] div[dir="auto"]',
            'div[data-ad-preview="message"] div[dir="auto"]',
            'div[class*="x1iorvi4"] div[dir="auto"]',
            'div[class*="xdj266r"] div[dir="auto"]',
            'div.x1iorvi4 span[dir="auto"]',
            'div[data-testid="post_message"] div',
        ]
        for sel in text_selectors:
            try:
                els = self.page.locator(sel).all()
                texts = []
                for el in els:
                    t = el.text_content(timeout=2000)
                    if t and len(t.strip()) > 5:
                        texts.append(t.strip())
                if texts:
                    # Get the longest text block (most likely the full ad copy)
                    full_text = max(texts, key=len)
                    return full_text
            except:
                continue

        return "N/A"

    def _extract_headline_description(self) -> tuple:
        """Extract headline and description from below the creative."""
        headline = "N/A"
        description = "N/A"

        # Headline is typically a bold link below the video/image
        headline_selectors = [
            'a[class*="x1i10hfl"] span[class*="x1lliihq"][dir="auto"]',
            'div[class*="xu06os2"] span[dir="auto"]',
            'span.x1lliihq.x6ikm8r',
        ]
        for sel in headline_selectors:
            try:
                els = self.page.locator(sel).all()
                for el in els:
                    text = el.text_content(timeout=2000)
                    if text and 5 < len(text.strip()) < 200:
                        headline = text.strip()
                        break
                if headline != "N/A":
                    break
            except:
                continue

        # Description is lighter text below headline
        desc_selectors = [
            'span.x1lliihq.x1plvlek',
            'div[class*="x1sxyh0"] span',
        ]
        for sel in desc_selectors:
            try:
                el = self.page.locator(sel).first
                text = el.text_content(timeout=2000)
                if text and len(text.strip()) > 5:
                    description = text.strip()
                    break
            except:
                continue

        return headline, description

    def _extract_cta(self) -> str:
        """Extract the call-to-action button text."""
        cta_selectors = [
            'div[class*="x1ja2u2z"] a span',
            'a[data-testid*="cta"] span',
            'div[role="button"] span:has-text("Learn More")',
            'div[role="button"] span:has-text("Sign Up")',
            'div[role="button"] span:has-text("Shop Now")',
            'div[role="button"] span:has-text("Book Now")',
            'div[role="button"] span:has-text("Get Offer")',
            'div[role="button"] span:has-text("Download")',
            'div[role="button"] span:has-text("Watch More")',
            'div[role="button"] span:has-text("Apply Now")',
            'div[role="button"] span:has-text("Contact Us")',
            'div[role="button"] span:has-text("Subscribe")',
            'a[class*="x1i10hfl"] div[class*="x1lliihq"]',
        ]

        cta_keywords = [
            "Learn More", "Sign Up", "Shop Now", "Book Now",
            "Get Offer", "Download", "Watch More", "Apply Now",
            "Contact Us", "Subscribe", "Order Now", "Get Started",
            "Send Message", "Get Quote", "See More", "Install Now",
            "Use App", "Play Game", "Listen Now", "Get Directions",
        ]

        for sel in cta_selectors:
            try:
                el = self.page.locator(sel).first
                text = el.text_content(timeout=2000)
                if text:
                    text = text.strip()
                    for kw in cta_keywords:
                        if kw.lower() in text.lower():
                            return kw
                    if len(text) < 30:
                        return text
            except:
                continue

        return "N/A"

    def _extract_landing_url(self) -> str:
        """Extract the destination URL the ad links to."""
        try:
            # Look for external links in the ad card area
            links = self.page.locator('a[href*="l.facebook.com/l.php"], a[rel="nofollow noopener"]').all()
            for link in links:
                href = link.get_attribute("href", timeout=2000)
                if href:
                    # Facebook wraps external URLs in a redirect
                    if "l.facebook.com/l.php" in href:
                        parsed = parse_qs(urlparse(href).query)
                        if "u" in parsed:
                            return parsed["u"][0]
                    elif "facebook.com" not in href:
                        return href
        except:
            pass

        return "N/A"

    def _extract_engagement(self) -> tuple:
        """Extract reactions, comments, and shares counts."""
        reactions = "N/A"
        comments = "N/A"
        shares = "N/A"

        try:
            # Reactions count
            react_selectors = [
                'span[aria-label*="reaction"]',
                'span[aria-label*="like"]',
                'span[class*="x1e558r4"]',
                'span[aria-label*="people reacted"]',
            ]
            for sel in react_selectors:
                try:
                    el = self.page.locator(sel).first
                    text = el.text_content(timeout=2000) or el.get_attribute("aria-label", timeout=1000)
                    if text:
                        nums = re.findall(r'[\d,]+', text)
                        if nums:
                            reactions = nums[0].replace(",", "")
                            break
                except:
                    continue

            # Comments count
            try:
                comment_el = self.page.locator('text=/\\d+\\s*comment/i').first
                text = comment_el.text_content(timeout=2000)
                nums = re.findall(r'[\d,]+', text)
                if nums:
                    comments = nums[0].replace(",", "")
            except:
                pass

            # Shares count
            try:
                share_el = self.page.locator('text=/\\d+\\s*share/i').first
                text = share_el.text_content(timeout=2000)
                nums = re.findall(r'[\d,]+', text)
                if nums:
                    shares = nums[0].replace(",", "")
            except:
                pass

        except:
            pass

        return reactions, comments, shares

    def _compute_total_engagement(self, reactions, comments, shares) -> str:
        """Sum engagement metrics if they're numeric."""
        total = 0
        found_any = False
        for val in [reactions, comments, shares]:
            try:
                total += int(str(val).replace(",", ""))
                found_any = True
            except (ValueError, TypeError):
                continue
        return str(total) if found_any else "N/A"

    def _extract_post_date(self) -> str:
        """Extract the post publication date."""
        date_selectors = [
            'a[href*="/posts/"] span',
            'span[id*="jsc_"] a[role="link"]',
            'a[aria-label*="20"] span',
            'abbr[data-utime]',
            'span[class*="x4k7w5x"]',
        ]
        for sel in date_selectors:
            try:
                el = self.page.locator(sel).first
                text = el.text_content(timeout=2000) or ""
                aria = el.get_attribute("aria-label", timeout=1000) or ""
                candidate = aria if len(aria) > len(text) else text
                if candidate and any(c.isdigit() for c in candidate):
                    return candidate.strip()
            except:
                continue
        return "N/A"

    def _extract_follower_count(self) -> str:
        """Extract page follower count if visible."""
        try:
            follower_el = self.page.locator('text=/\\d.*follower/i').first
            text = follower_el.text_content(timeout=2000)
            if text:
                return text.strip()
        except:
            pass
        return "N/A"

    def _detect_video(self) -> bool:
        """Check if the post contains a video."""
        video_indicators = [
            'video',
            'div[data-video-id]',
            'div[class*="video"]',
            'div[aria-label*="video"]',
            'i[class*="playButton"]',
            'div[data-testid="video_player"]',
            'div[class*="__fb-video"]',
        ]
        for sel in video_indicators:
            try:
                el = self.page.locator(sel).first
                if el.is_visible(timeout=2000):
                    return True
            except:
                continue

        # Check page source for video URLs
        content = self.page.content()
        video_patterns = [
            'video_url', 'playable_url', '.mp4', 'video_id',
            'videoID', 'data-video', 'video src=',
        ]
        for pattern in video_patterns:
            if pattern in content:
                return True

        return False

    def _extract_video_url(self) -> str:
        """Extract the video download URL from the page."""
        # Method 1: Direct video src
        try:
            video_el = self.page.locator("video source, video").first
            src = video_el.get_attribute("src", timeout=3000)
            if src and (".mp4" in src or "video" in src):
                return src
        except:
            pass

        # Method 2: Extract from page source using regex
        content = self.page.content()

        # Look for HD video URL first, then SD
        patterns = [
            r'"playable_url_quality_hd"\s*:\s*"([^"]+)"',
            r'"browser_native_hd_url"\s*:\s*"([^"]+)"',
            r'"playable_url"\s*:\s*"([^"]+)"',
            r'"browser_native_sd_url"\s*:\s*"([^"]+)"',
            r'"sd_src"\s*:\s*"([^"]+)"',
            r'"hd_src"\s*:\s*"([^"]+)"',
            r'"video_url"\s*:\s*"([^"]+)"',
            r'<video[^>]+src="([^"]+)"',
            r'"contentUrl"\s*:\s*"([^"]+\.mp4[^"]*)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                # Unescape unicode
                url = match.replace("\\u0025", "%").replace("\\/", "/").replace("\\u0026", "&")
                if url and ("video" in url or ".mp4" in url):
                    return url

        # Method 3: Intercept network requests for video
        try:
            # Click play if needed
            play_buttons = [
                'div[aria-label="Play"]',
                'div[data-testid="play_button"]',
                'i[class*="play"]',
            ]
            for sel in play_buttons:
                try:
                    btn = self.page.locator(sel).first
                    if btn.is_visible(timeout=1000):
                        btn.click(timeout=2000)
                        time.sleep(2)
                        break
                except:
                    continue

            # Re-check page content after clicking play
            content = self.page.content()
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    url = match.replace("\\u0025", "%").replace("\\/", "/").replace("\\u0026", "&")
                    if url and ("video" in url or ".mp4" in url):
                        return url
        except:
            pass

        return ""

    def close(self):
        """Clean up browser resources."""
        try:
            if self.context:
                self.context.close()
            if self.playwright:
                self.playwright.stop()
            self.logger.info("Browser closed")
        except:
            pass

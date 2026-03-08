"""
Video download, thumbnail extraction, and media processing.
"""

import os
import re
import subprocess
import time
import requests
from urllib.parse import unquote
from .config import ScraperConfig
from .logger import ScrapeLogger


class VideoProcessor:
    """Downloads videos, extracts thumbnails, and handles media processing."""

    def __init__(self, config: ScraperConfig, logger: ScrapeLogger):
        self.config = config
        self.logger = logger

    def download_video(self, video_url: str, index: int, advertiser_name: str) -> str:
        """
        Download a video file from the extracted URL.
        Returns local file path or empty string on failure.
        """
        if not video_url:
            self.logger.warning("No video URL available for download")
            return ""

        # Sanitize advertiser name for filename
        safe_name = re.sub(r'[^\w\s-]', '', advertiser_name).strip().replace(' ', '-')[:50]
        if not safe_name:
            safe_name = "unknown"

        filename = f"ad_{index:03d}_{safe_name}.mp4"
        filepath = os.path.join(self.config.videos_dir, filename)

        try:
            self.logger.info(f"Downloading video: {filename}")

            # Use requests with headers to download
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Referer": "https://www.facebook.com/",
            }

            response = requests.get(video_url, headers=headers, stream=True, timeout=120)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            file_size = os.path.getsize(filepath)
            if file_size < 10000:  # Less than 10KB is probably an error page
                self.logger.warning(f"Downloaded file too small ({file_size} bytes), likely not a video")
                os.remove(filepath)
                return ""

            size_mb = file_size / (1024 * 1024)
            self.logger.success(f"Video downloaded: {filename} ({size_mb:.1f} MB)")
            return filepath

        except Exception as e:
            self.logger.error(f"Video download failed: {str(e)}")
            if os.path.exists(filepath):
                os.remove(filepath)
            return ""

    def download_video_yt_dlp(self, page_url: str, index: int, advertiser_name: str) -> str:
        """
        Fallback: Use yt-dlp to download the video from the Facebook page URL.
        Requires yt-dlp installed and Facebook cookies.
        """
        safe_name = re.sub(r'[^\w\s-]', '', advertiser_name).strip().replace(' ', '-')[:50]
        if not safe_name:
            safe_name = "unknown"

        filename = f"ad_{index:03d}_{safe_name}.mp4"
        filepath = os.path.join(self.config.videos_dir, filename)

        try:
            self.logger.info(f"Trying yt-dlp fallback for: {filename}")

            # Export cookies from browser_data for yt-dlp
            cookies_file = os.path.join(self.config.browser_data_dir, "cookies.txt")

            cmd = [
                "yt-dlp",
                "--no-check-certificates",
                "-o", filepath,
                "--merge-output-format", "mp4",
                "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "--no-playlist",
                "--socket-timeout", "30",
            ]

            if os.path.exists(cookies_file):
                cmd.extend(["--cookies", cookies_file])

            cmd.append(page_url)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode == 0 and os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                if file_size > 10000:
                    size_mb = file_size / (1024 * 1024)
                    self.logger.success(f"Video downloaded via yt-dlp: {filename} ({size_mb:.1f} MB)")
                    return filepath

            self.logger.warning(f"yt-dlp failed: {result.stderr[:200] if result.stderr else 'unknown error'}")
            return ""

        except subprocess.TimeoutExpired:
            self.logger.warning("yt-dlp download timed out")
            return ""
        except FileNotFoundError:
            self.logger.warning("yt-dlp not installed, skipping fallback")
            return ""
        except Exception as e:
            self.logger.error(f"yt-dlp error: {str(e)}")
            return ""

    def download_dash_video(self, video_url: str, audio_url: str, index: int, advertiser_name: str) -> str:
        """
        Download DASH video + audio streams separately, then mux with ffmpeg.
        Facebook DASH manifests provide separate video/audio CDN URLs.
        Returns local file path or empty string on failure.
        """
        if not video_url:
            self.logger.warning("No DASH video URL available")
            return ""

        safe_name = re.sub(r'[^\w\s-]', '', advertiser_name).strip().replace(' ', '-')[:50]
        if not safe_name:
            safe_name = "unknown"

        filepath = os.path.join(self.config.videos_dir, f"ad_{index:03d}_{safe_name}.mp4")
        video_tmp = os.path.join(self.config.videos_dir, f"_tmp_video_{index:03d}.mp4")
        audio_tmp = os.path.join(self.config.videos_dir, f"_tmp_audio_{index:03d}.m4a")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.facebook.com/",
        }

        try:
            # Download video stream
            self.logger.info(f"Downloading DASH video stream...")
            resp = requests.get(video_url, headers=headers, stream=True, timeout=120)
            resp.raise_for_status()
            with open(video_tmp, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            video_size = os.path.getsize(video_tmp)
            if video_size < 10000:
                self.logger.warning(f"Video stream too small ({video_size} bytes)")
                self._cleanup_tmp(video_tmp, audio_tmp)
                return ""

            # Download audio stream (if available)
            has_audio = False
            if audio_url:
                self.logger.info(f"Downloading DASH audio stream...")
                try:
                    resp = requests.get(audio_url, headers=headers, stream=True, timeout=60)
                    resp.raise_for_status()
                    with open(audio_tmp, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    if os.path.getsize(audio_tmp) > 1000:
                        has_audio = True
                except Exception as e:
                    self.logger.warning(f"Audio stream download failed: {str(e)}")

            # Mux video + audio with ffmpeg
            if has_audio:
                self.logger.info("Muxing video + audio streams...")
                cmd = [
                    "ffmpeg", "-y",
                    "-i", video_tmp,
                    "-i", audio_tmp,
                    "-c:v", "copy",
                    "-c:a", "aac",
                    "-movflags", "+faststart",
                    filepath,
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

                if result.returncode != 0 or not os.path.exists(filepath):
                    self.logger.warning(f"ffmpeg mux failed: {result.stderr[:200]}")
                    # Fall back to video-only
                    os.rename(video_tmp, filepath)
            else:
                # Video only — just rename
                os.rename(video_tmp, filepath)

            self._cleanup_tmp(video_tmp, audio_tmp)

            if os.path.exists(filepath) and os.path.getsize(filepath) > 10000:
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                self.logger.success(f"DASH video saved: ad_{index:03d}_{safe_name}.mp4 ({size_mb:.1f} MB)")
                return filepath

            return ""

        except Exception as e:
            self.logger.error(f"DASH video download failed: {str(e)}")
            self._cleanup_tmp(video_tmp, audio_tmp)
            if os.path.exists(filepath):
                os.remove(filepath)
            return ""

    def _cleanup_tmp(self, *paths):
        """Remove temporary files."""
        for p in paths:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass

    def extract_thumbnail(self, video_path: str, index: int) -> str:
        """Extract first frame from video as thumbnail."""
        if not video_path or not os.path.exists(video_path):
            return ""

        thumbnail_path = os.path.join(self.config.thumbnails_dir, f"ad_{index:03d}_thumb.png")

        try:
            self.logger.info("Extracting thumbnail...")

            cmd = [
                "ffmpeg",
                "-y",
                "-i", video_path,
                "-vframes", "1",
                "-q:v", "2",
                "-f", "image2",
                thumbnail_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
                self.logger.success(f"Thumbnail saved: ad_{index:03d}_thumb.png")
                return thumbnail_path

            self.logger.warning("Thumbnail extraction failed")
            return ""

        except Exception as e:
            self.logger.error(f"Thumbnail error: {str(e)}")
            return ""

    def extract_audio(self, video_path: str) -> str:
        """Extract audio track from video for transcription."""
        if not video_path or not os.path.exists(video_path):
            return ""

        audio_path = video_path.replace(".mp4", ".wav")

        try:
            cmd = [
                "ffmpeg",
                "-y",
                "-i", video_path,
                "-vn",
                "-acodec", "pcm_s16le",
                "-ar", "16000",
                "-ac", "1",
                audio_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if os.path.exists(audio_path) and os.path.getsize(audio_path) > 0:
                return audio_path

            return ""

        except Exception as e:
            self.logger.error(f"Audio extraction error: {str(e)}")
            return ""

    def get_video_info(self, video_path: str) -> dict:
        """Get video metadata using ffprobe."""
        info = {
            "duration": 0,
            "duration_str": "N/A",
            "width": 0,
            "height": 0,
            "resolution": "N/A",
            "orientation": "N/A",
        }

        if not video_path or not os.path.exists(video_path):
            return info

        try:
            # Get duration
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                video_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)

                # Duration
                duration = float(data.get("format", {}).get("duration", 0))
                info["duration"] = duration
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                info["duration_str"] = f"{minutes}:{seconds:02d}"

                # Resolution from video stream
                for stream in data.get("streams", []):
                    if stream.get("codec_type") == "video":
                        w = int(stream.get("width", 0))
                        h = int(stream.get("height", 0))
                        info["width"] = w
                        info["height"] = h
                        info["resolution"] = f"{w}x{h}"

                        # Determine orientation
                        if w > 0 and h > 0:
                            ratio = w / h
                            if abs(ratio - 9/16) < 0.1 or h > w * 1.3:
                                info["orientation"] = "Vertical (9:16)"
                            elif abs(ratio - 1.0) < 0.15:
                                info["orientation"] = "Square (1:1)"
                            elif abs(ratio - 16/9) < 0.2 or w > h * 1.3:
                                info["orientation"] = "Horizontal (16:9)"
                            else:
                                info["orientation"] = f"Custom ({w}:{h})"
                        break

        except Exception as e:
            self.logger.warning(f"ffprobe error: {str(e)}")

        return info

    def cleanup_audio(self, video_path: str):
        """Remove temporary audio file."""
        audio_path = video_path.replace(".mp4", ".wav")
        if os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except:
                pass

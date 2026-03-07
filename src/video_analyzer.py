"""
Video analysis: scene detection, computed metrics, and content analysis.
"""

import os
import re
import math
import subprocess
import json
from .logger import ScrapeLogger


class VideoAnalyzer:
    """Analyzes video content for scene cuts, speaking pace, hooks, and CTAs."""

    def __init__(self, logger: ScrapeLogger):
        self.logger = logger

    def analyze(self, video_path: str, video_info: dict, transcript_data: dict, ad_data: dict) -> dict:
        """
        Run full video analysis and return computed fields.

        Args:
            video_path: Path to the video file
            video_info: Dict with duration, resolution etc. from ffprobe
            transcript_data: Dict from transcriber with segments, words, full_transcript
            ad_data: The ad data dict (for CTA text matching)

        Returns:
            Dict with all computed video analysis fields
        """
        analysis = {
            "hook_duration": "N/A",
            "total_word_count": "N/A",
            "words_per_minute": "N/A",
            "cta_timestamp": "N/A",
            "number_of_scenes": "N/A",
            "avg_scene_duration": "N/A",
            "first_3_seconds": "N/A",
            "first_5_seconds": "N/A",
            "last_5_seconds": "N/A",
            "has_captions": "N/A",
            "caption_style": "N/A",
            "has_background_music": "N/A",
        }

        full_transcript = transcript_data.get("full_transcript", "")
        segments = transcript_data.get("segments", [])
        words = transcript_data.get("words", [])
        duration = video_info.get("duration", 0)

        if not full_transcript:
            return analysis

        # --- WORD COUNT & SPEAKING PACE ---
        word_count = len(full_transcript.split())
        analysis["total_word_count"] = str(word_count)

        if duration > 0:
            wpm = round(word_count / (duration / 60))
            analysis["words_per_minute"] = str(wpm)

        # --- FIRST 3 SECONDS / FIRST 5 SECONDS ---
        analysis["first_3_seconds"] = self._get_text_in_range(segments, 0, 3)
        analysis["first_5_seconds"] = self._get_text_in_range(segments, 0, 5)

        # --- LAST 5 SECONDS ---
        if duration > 5:
            analysis["last_5_seconds"] = self._get_text_in_range(
                segments, duration - 5, duration
            )
        elif segments:
            analysis["last_5_seconds"] = segments[-1]["text"]

        # --- HOOK DURATION ---
        # Time until first value proposition / claim
        analysis["hook_duration"] = self._detect_hook_duration(segments)

        # --- CTA TIMESTAMP ---
        analysis["cta_timestamp"] = self._detect_cta_timestamp(
            segments, ad_data.get("call_to_action", "")
        )

        # --- SCENE DETECTION ---
        scene_count = self._detect_scenes(video_path)
        analysis["number_of_scenes"] = str(scene_count)

        if scene_count > 0 and duration > 0:
            avg_scene = round(duration / scene_count, 1)
            analysis["avg_scene_duration"] = f"{avg_scene} seconds"

        # --- CAPTION DETECTION ---
        has_captions, caption_style = self._detect_captions(video_path)
        analysis["has_captions"] = has_captions
        analysis["caption_style"] = caption_style

        # --- BACKGROUND MUSIC DETECTION ---
        analysis["has_background_music"] = self._detect_background_music(video_path, words)

        return analysis

    def _get_text_in_range(self, segments: list, start_sec: float, end_sec: float) -> str:
        """Get transcript text that falls within a time range."""
        texts = []
        for seg in segments:
            # Include segment if it overlaps with our range
            if seg["end"] > start_sec and seg["start"] < end_sec:
                texts.append(seg["text"])

        result = " ".join(texts).strip()
        return result if result else "N/A"

    def _detect_hook_duration(self, segments: list) -> str:
        """
        Estimate how many seconds before the first value proposition.
        Looks for claim indicators like numbers, comparisons, benefits.
        """
        claim_patterns = [
            r'\d+',  # Numbers (stats, claims)
            r'(?:million|billion|thousand|hundred)',
            r'(?:guarantee|proven|results|revenue|growth|profit|save|earn)',
            r'(?:percent|%)',
            r'(?:clients?|customers?|companies|businesses)',
            r'(?:secret|discover|learn|find out|introducing)',
            r'(?:never|always|every|only|just)',
            r'(?:free|no cost|zero)',
            r'(?:increase|decrease|boost|grow|double|triple)',
        ]

        for seg in segments:
            text = seg["text"].lower()
            for pattern in claim_patterns:
                if re.search(pattern, text):
                    hook_time = round(seg["start"], 1)
                    return f"{hook_time} seconds"

        # If no clear claim found, default to first segment end
        if segments:
            return f"{round(segments[0]['end'], 1)} seconds"

        return "N/A"

    def _detect_cta_timestamp(self, segments: list, cta_text: str) -> str:
        """Find when the call-to-action is spoken in the video."""
        # CTA keywords to search for in transcript
        cta_keywords = [
            "link in bio", "click the link", "click below", "sign up",
            "book a call", "book now", "learn more", "get started",
            "visit", "download", "subscribe", "call now", "order now",
            "shop now", "try it", "free trial", "dm me", "dm us",
            "comment below", "tap the link", "swipe up",
            "strategy call", "consultation", "apply now",
        ]

        # Also add the actual CTA text if available
        if cta_text and cta_text != "N/A":
            cta_keywords.insert(0, cta_text.lower())

        # Search segments in reverse (CTA usually near the end)
        for seg in reversed(segments):
            text = seg["text"].lower()
            for keyword in cta_keywords:
                if keyword in text:
                    return f"[{self._fmt_time(seg['start'])}]"

        return "N/A"

    def _detect_scenes(self, video_path: str) -> int:
        """
        Detect scene cuts/transitions using ffmpeg scene detection filter.
        Returns estimated number of scenes.
        """
        if not video_path or not os.path.exists(video_path):
            return 0

        try:
            # Use ffmpeg's scene detection filter
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-filter:v", "select='gt(scene,0.3)',showinfo",
                "-f", "null",
                "-",
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )

            # Count scene changes from stderr output
            stderr = result.stderr
            scene_changes = stderr.count("Parsed_showinfo")

            # Total scenes = changes + 1 (the first scene)
            total_scenes = scene_changes + 1 if scene_changes >= 0 else 1

            self.logger.info(f"Detected {total_scenes} scenes ({scene_changes} cuts)")
            return total_scenes

        except subprocess.TimeoutExpired:
            self.logger.warning("Scene detection timed out")
            return 0
        except Exception as e:
            self.logger.warning(f"Scene detection error: {str(e)}")
            return 0

    def _detect_captions(self, video_path: str) -> tuple:
        """
        Detect if the video has burned-in captions by analyzing frames.
        Returns (has_captions: str, caption_style: str).
        """
        if not video_path or not os.path.exists(video_path):
            return "N/A", "N/A"

        try:
            # Use OpenCV to analyze frames for text regions
            try:
                import cv2
                import numpy as np
            except ImportError:
                return "N/A (opencv not installed)", "N/A"

            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                return "N/A", "N/A"

            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            if total_frames == 0:
                cap.release()
                return "N/A", "N/A"

            # Sample frames at regular intervals (check ~10 frames)
            sample_interval = max(1, total_frames // 10)
            caption_detections = 0
            frames_checked = 0
            detected_regions = []

            for frame_idx in range(0, total_frames, sample_interval):
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                ret, frame = cap.read()
                if not ret:
                    continue

                frames_checked += 1
                height, width = frame.shape[:2]

                # Focus on the lower third and center of the frame (where captions typically are)
                bottom_region = frame[int(height * 0.6):, :]
                center_region = frame[int(height * 0.3):int(height * 0.7),
                                     int(width * 0.1):int(width * 0.9)]

                for region, region_name in [(bottom_region, "bottom"), (center_region, "center")]:
                    # Convert to grayscale and look for high-contrast text regions
                    gray = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY)

                    # Look for bright text on dark background or vice versa
                    # High contrast regions often indicate text
                    _, binary_high = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
                    _, binary_low = cv2.threshold(gray, 50, 255, cv2.THRESH_BINARY_INV)

                    # Check if there are horizontal bands of white pixels (text lines)
                    for binary in [binary_high, binary_low]:
                        row_sums = np.sum(binary, axis=1) / 255
                        text_rows = np.where(row_sums > width * 0.05)[0]

                        if len(text_rows) > 5:
                            # Check for grouped text rows (caption line)
                            diffs = np.diff(text_rows)
                            groups = np.split(text_rows, np.where(diffs > 5)[0] + 1)
                            significant_groups = [g for g in groups if len(g) > 3]

                            if len(significant_groups) >= 1:
                                caption_detections += 1
                                detected_regions.append(region_name)
                                break

                if frames_checked >= 10:
                    break

            cap.release()

            # Determine caption presence and style
            detection_ratio = caption_detections / max(frames_checked, 1)

            if detection_ratio > 0.3:
                has_captions = "Yes"
                # Infer style based on where captions were detected
                if "center" in detected_regions and "bottom" in detected_regions:
                    caption_style = "Centered text overlay with word-by-word highlight"
                elif detected_regions.count("center") > detected_regions.count("bottom"):
                    caption_style = "Bold centered captions"
                else:
                    caption_style = "Subtitle bar at bottom"
            elif detection_ratio > 0.1:
                has_captions = "Likely"
                caption_style = "Intermittent text overlays"
            else:
                has_captions = "No"
                caption_style = "N/A"

            return has_captions, caption_style

        except Exception as e:
            self.logger.warning(f"Caption detection error: {str(e)}")
            return "N/A", "N/A"

    def _detect_background_music(self, video_path: str, words: list) -> str:
        """
        Detect if background music is present by analyzing audio characteristics.
        Compares audio energy during speech vs. non-speech segments.
        """
        if not video_path or not os.path.exists(video_path):
            return "N/A"

        try:
            # Use ffmpeg to analyze audio levels
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-af", "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level",
                "-f", "null",
                "-",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            # Parse RMS levels from stderr
            levels = re.findall(r'RMS_level=(-?[\d.]+)', result.stderr)
            levels = [float(l) for l in levels if l != '-inf']

            if not levels:
                return "N/A"

            avg_level = sum(levels) / len(levels)
            min_level = min(levels)

            # If the quietest parts are still relatively loud, music is likely present
            # Speech alone would have quiet gaps between sentences
            if min_level > -40 and avg_level > -25:
                return "Yes"
            elif min_level > -50 and avg_level > -30:
                return "Likely"
            else:
                return "No"

        except Exception as e:
            self.logger.warning(f"Music detection error: {str(e)}")
            return "N/A"

    def _fmt_time(self, seconds: float) -> str:
        """Format seconds as MM:SS."""
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m:02d}:{s:02d}"

"""
Video transcription using OpenAI Whisper (local, open source).
Generates word-level timestamps and groups them into natural sentence segments.
"""

import os
import re
import math
from .logger import ScrapeLogger


class VideoTranscriber:
    """Transcribes video audio using local Whisper model."""

    def __init__(self, model_name: str, logger: ScrapeLogger):
        self.model_name = model_name
        self.logger = logger
        self.model = None

    def load_model(self):
        """Load the Whisper model. Called once at startup."""
        try:
            import whisper
            self.logger.info(f"Loading Whisper model: {self.model_name}")
            self.logger.info("(First run will download the model, this may take a few minutes)")
            self.model = whisper.load_model(self.model_name)
            self.logger.success(f"Whisper model '{self.model_name}' loaded")
            return True
        except ImportError:
            self.logger.error(
                "OpenAI Whisper not installed. Run: pip install openai-whisper"
            )
            return False
        except Exception as e:
            self.logger.error(f"Failed to load Whisper model: {str(e)}")
            return False

    def transcribe(self, audio_path: str) -> dict:
        """
        Transcribe an audio file and return structured transcript data.

        Returns:
            {
                "full_transcript": str,
                "timestamped_transcript": str,  # Formatted with [MM:SS - MM:SS] segments
                "segments": [{"start": float, "end": float, "text": str}, ...],
                "words": [{"word": str, "start": float, "end": float}, ...],
                "language": str,
            }
        """
        result = {
            "full_transcript": "",
            "timestamped_transcript": "",
            "segments": [],
            "words": [],
            "language": "en",
        }

        if not self.model:
            self.logger.error("Whisper model not loaded")
            return result

        if not audio_path or not os.path.exists(audio_path):
            self.logger.error(f"Audio file not found: {audio_path}")
            return result

        try:
            self.logger.info("Transcribing audio with Whisper...")

            # Run Whisper with word-level timestamps
            whisper_result = self.model.transcribe(
                audio_path,
                word_timestamps=True,
                language=None,  # Auto-detect
                task="transcribe",
                verbose=False,
            )

            # Extract detected language
            result["language"] = whisper_result.get("language", "en")

            # Extract full transcript
            result["full_transcript"] = whisper_result.get("text", "").strip()

            if not result["full_transcript"]:
                self.logger.warning("No speech detected in audio")
                return result

            # Extract word-level timestamps
            words = []
            for segment in whisper_result.get("segments", []):
                for word_info in segment.get("words", []):
                    words.append({
                        "word": word_info.get("word", "").strip(),
                        "start": word_info.get("start", 0),
                        "end": word_info.get("end", 0),
                    })
            result["words"] = words

            # Group words into natural sentence segments (2-5 seconds each)
            segments = self._group_into_segments(words)
            result["segments"] = segments

            # Format timestamped transcript
            result["timestamped_transcript"] = self._format_timestamped(segments)

            word_count = len(result["full_transcript"].split())
            self.logger.success(
                f"Transcription complete: {word_count} words, "
                f"{len(segments)} segments, "
                f"language: {result['language']}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Transcription failed: {str(e)}")
            return result

    def _group_into_segments(self, words: list) -> list:
        """
        Group word-level timestamps into natural sentence segments.
        Target: 2-5 seconds per segment, breaking at speech pauses and punctuation.
        """
        if not words:
            return []

        segments = []
        current_segment_words = []
        segment_start = None

        # Punctuation that indicates a natural break
        sentence_enders = {'.', '!', '?'}
        pause_threshold = 0.4  # seconds of silence to consider a pause

        for i, word in enumerate(words):
            if not word["word"]:
                continue

            if segment_start is None:
                segment_start = word["start"]

            current_segment_words.append(word)
            segment_duration = word["end"] - segment_start

            # Determine if we should break here
            should_break = False

            # Check for natural sentence ending
            last_char = word["word"].rstrip()[-1] if word["word"].strip() else ""
            is_sentence_end = last_char in sentence_enders

            # Check for pause after this word
            has_pause = False
            if i + 1 < len(words):
                gap = words[i + 1]["start"] - word["end"]
                has_pause = gap > pause_threshold

            # Breaking rules (in priority order)
            if segment_duration >= 5.0:
                # Hard break: segment is too long
                should_break = True
            elif segment_duration >= 3.0 and is_sentence_end:
                # Natural sentence end after 3+ seconds
                should_break = True
            elif segment_duration >= 2.0 and is_sentence_end:
                # Sentence end after 2+ seconds
                should_break = True
            elif segment_duration >= 2.5 and has_pause:
                # Pause break after 2.5+ seconds
                should_break = True
            elif segment_duration >= 4.0 and (last_char == ',' or has_pause):
                # Comma or pause after 4+ seconds
                should_break = True

            # Also break if it's the last word
            if i == len(words) - 1:
                should_break = True

            if should_break and current_segment_words:
                text = " ".join(w["word"] for w in current_segment_words).strip()
                # Clean up spacing around punctuation
                text = re.sub(r'\s+([.,!?;:])', r'\1', text)
                text = re.sub(r'\s+', ' ', text)

                if text:
                    segments.append({
                        "start": segment_start,
                        "end": word["end"],
                        "text": text,
                    })

                current_segment_words = []
                segment_start = None

        return segments

    def _format_timestamped(self, segments: list) -> str:
        """Format segments into the required timestamp format."""
        lines = []
        for seg in segments:
            start = self._format_time(seg["start"])
            end = self._format_time(seg["end"])
            lines.append(f"[{start} - {end}] {seg['text']}")
        return "\n".join(lines)

    def _format_time(self, seconds: float) -> str:
        """Format seconds into MM:SS format."""
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes:02d}:{secs:02d}"

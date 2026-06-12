"""Structured progress signal for the task-progress UI.

A running pipeline writes a small ``progress.json`` into its per-run log
directory (alongside ``run.txt``). The Django ``task_status`` view reads it
back and serves it to the task-progress page, which renders a staged progress
bar from it.

The payload is intentionally minimal — only the current stage and its internal
completion fraction. The frontend fills every stage *before* the current one as
100%, so stages never need to report their own completion.
"""

import json
import logging
import os
import time


class ProgressReporter:
    """Writes a throttled, atomically-replaced ``progress.json``.

    Constructed with the run's log directory and the ordered list of
    ``(stage_key, label)`` markers that apply to this pipeline. Stages call
    :meth:`update` as they work; writes are throttled so tight loops (per-query,
    per-cell) don't hammer the disk, but a stage-key change always flushes.

    All IO is best-effort: a failed write is logged and swallowed so progress
    reporting can never break a job.
    """

    _MIN_WRITE_INTERVAL = 0.4  # seconds between writes within the same stage

    def __init__(self, log_dir: str, stages: list[tuple[str, str]]) -> None:
        self._path = os.path.join(log_dir, "progress.json")
        self._stages = [{"key": key, "label": label} for key, label in stages]
        self._last_write = 0.0
        self._last_stage: str | None = None

    def update(self, stage_key: str, fraction: float, status_text: str) -> None:
        fraction = max(0.0, min(1.0, fraction))

        # Always flush on a stage change or a stage's final (100%) update;
        # otherwise throttle so tight loops don't hammer the disk.
        now = time.monotonic()
        must_write = stage_key != self._last_stage or fraction >= 1.0
        if not must_write and now - self._last_write < self._MIN_WRITE_INTERVAL:
            return

        payload = {
            "stages": self._stages,
            "current_stage": stage_key,
            "fraction": fraction,
            "status_text": status_text,
        }

        try:
            tmp_path = f"{self._path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(payload, f)
            os.replace(tmp_path, self._path)
            self._last_write = now
            self._last_stage = stage_key
        except OSError as e:
            logging.warning("Failed to write progress.json: %s", e)

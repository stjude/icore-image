import collections
import logging
import os
import re
import subprocess
import sys
import tempfile
from abc import ABC
from urllib.parse import urlparse

from pipeline.base import PipelineStage
from pipeline.context import PipelineContext

# rclone's ``--stats-one-line`` output carries the overall completion percent,
# e.g. ``Transferred: 1.5 MiB / 10 MiB, 15%, 500 KiB/s, ETA 20s``.
_RCLONE_PERCENT_RE = re.compile(r"(\d+)%")


def _parse_rclone_percent(line: str) -> float | None:
    """Return the completion fraction (0..1) from an rclone stats line.

    ``None`` when the line carries no percentage (e.g. an unrelated log line).
    """
    match = _RCLONE_PERCENT_RE.search(line)
    if not match:
        return None
    return max(0.0, min(1.0, int(match.group(1)) / 100.0))


def _get_rclone_binary() -> str:
    if getattr(sys, "frozen", False):
        bundle_dir = os.path.abspath(os.path.dirname(sys.executable))
        rclone_binary = os.path.join(bundle_dir, "_internal", "rclone", "rclone")
    else:
        rclone_binary = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "rclone",
            "rclone",
        )

    return rclone_binary


def _parse_sas_url(sas_url: str) -> tuple[str, str, str]:
    parsed = urlparse(sas_url)

    is_azurite = parsed.netloc.startswith("127.0.0.1") or parsed.netloc.startswith(
        "localhost"
    )

    if is_azurite:
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) >= 2:
            account_name = path_parts[0]
            container_name = path_parts[1]
        else:
            account_name = "devstoreaccount1"
            container_name = path_parts[0] if path_parts else ""
    else:
        hostname = parsed.netloc
        account_name = hostname.split(".")[0]

        container_name = parsed.path.lstrip("/")

    sas_token = parsed.query

    return account_name, container_name, sas_token


def _create_rclone_config(sas_url: str, config_path: str) -> None:
    parsed = urlparse(sas_url)

    is_azurite = parsed.netloc.startswith("127.0.0.1") or parsed.netloc.startswith(
        "localhost"
    )

    if is_azurite:
        path_parts = [p for p in parsed.path.split("/") if p]
        account_name = path_parts[0] if path_parts else "devstoreaccount1"
        endpoint = f"{parsed.scheme}://{parsed.netloc}/{account_name}"

        config_content = f"""[azure]
            type = azureblob
            account = {account_name}
            use_emulator = true
            endpoint = {endpoint}
            sas_url = {sas_url}
        """
    else:
        account_name = parsed.netloc.split(".")[0]
        config_content = f"""[azure]
            type = azureblob
            account = {account_name}
            sas_url = {sas_url}
        """

    with open(config_path, "w") as f:
        f.write(config_content)


class ExportStage(PipelineStage, ABC):
    """Stage 4: push ``ctx.output_dir`` contents to a destination."""


class AzureBlobExport(ExportStage):
    """Upload ``ctx.output_dir`` to Azure Blob Storage via rclone.

    When *gate_on_content* is True, the stage is a no-op if neither
    images nor text rows were produced upstream — matching the "nothing
    to export, skip" behavior of the pre-refactor wrappers.
    """

    progress_marker = ("export", "Exporting to cloud")

    def __init__(
        self,
        sas_url: str,
        project_name: str,
        gate_on_content: bool = False,
    ) -> None:
        self.sas_url = sas_url
        self.project_name = project_name
        self.gate_on_content = gate_on_content

    def execute(self, ctx: PipelineContext) -> None:
        if (
            self.gate_on_content
            and ctx.images_saved == 0
            and ctx.text_rows_processed == 0
        ):
            logging.info("No content to export - skipping Azure upload")
            return

        logging.info("=" * 80)
        logging.info("IMAGE EXPORT MODULE (rclone)")
        logging.info("=" * 80)
        logging.info(f"Input directory: {ctx.output_dir}")
        logging.info(f"Project name: {self.project_name}")
        logging.info(f"SAS URL: {self.sas_url.split('?')[0]}...")
        logging.info("=" * 80)

        if not os.path.exists(ctx.output_dir):
            raise Exception(f"Input directory does not exist: {ctx.output_dir}")

        if not os.listdir(ctx.output_dir):
            raise Exception(f"Input directory is empty: {ctx.output_dir}")

        try:
            _, container_name, _ = _parse_sas_url(self.sas_url)

            rclone_config_path = os.path.join(
                tempfile.gettempdir(), f"rclone_{os.getpid()}.conf"
            )
            _create_rclone_config(self.sas_url, rclone_config_path)

            try:
                destination = f"azure:{container_name}/{self.project_name}"

                rclone_binary = _get_rclone_binary()
                # --stats-one-line + a fixed interval gives one machine-parseable
                # progress line per second (raised to NOTICE so it prints at the
                # default log level); we stream it into the progress bar.
                cmd = [
                    rclone_binary,
                    "copy",
                    "--stats",
                    "1s",
                    "--stats-one-line",
                    "--stats-log-level",
                    "NOTICE",
                    "--config",
                    rclone_config_path,
                    ctx.output_dir,
                    destination,
                ]

                logging.info(
                    f"Running rclone command: {' '.join(cmd[:2])} ... {destination}"
                )
                self._run_rclone(cmd, ctx)
                logging.info("PROGRESS: COMPLETE")
                ctx.export_performed = True
            finally:
                if os.path.exists(rclone_config_path):
                    os.remove(rclone_config_path)
        except Exception as e:
            error_msg = f"Error during export: {str(e)}"
            logging.error(error_msg)
            raise

    def _run_rclone(self, cmd: list[str], ctx: PipelineContext) -> None:
        """Run rclone, streaming its stats into ``ctx.progress`` as they arrive.

        stderr is merged into stdout so both the stats lines and any error
        output arrive in order; the last lines are retained for the exception
        message if rclone exits non-zero.
        """
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        tail: collections.deque[str] = collections.deque(maxlen=50)
        assert process.stdout is not None
        for raw in process.stdout:
            line = raw.rstrip("\n")
            if not line:
                continue
            tail.append(line)
            fraction = _parse_rclone_percent(line)
            if fraction is not None and ctx.progress:
                ctx.progress.update("export", fraction, "Exporting to cloud storage…")
        returncode = process.wait()
        if returncode != 0:
            details = "\n".join(tail)
            raise Exception(
                f"rclone error: Command failed with exit code {returncode}:\n{details}"
            )
        if ctx.progress:
            ctx.progress.update("export", 1.0, "Export complete")

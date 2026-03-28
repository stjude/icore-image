"""Wrapper for the dicom-deid-rs Rust de-identification engine."""

import logging
import os
import re
import subprocess
import sys
import tempfile

from recipe_translator import build_recipe
from utils import ImageDeidLocalResult


def _get_default_binary_path() -> str:
    """Locate the dicom-deid-rs binary."""
    if getattr(sys, "frozen", False):
        # Packaged app: binary is bundled alongside the executable
        base_dir = os.path.dirname(sys.executable)
        return os.path.join(base_dir, "dicom-deid-rs")

    # Development: use the cargo build output
    return os.path.join(
        os.path.dirname(__file__), "dicom-deid-rs", "target", "release", "dicom-deid-rs"
    )


def _get_default_pixel_script() -> str | None:
    """Load the default CTP pixel anonymizer script."""
    script_path = os.path.join(
        os.path.dirname(__file__), "ctp", "scripts", "DicomPixelAnonymizer.script"
    )
    if os.path.exists(script_path):
        with open(script_path, "r") as f:
            return f.read()
    return None


class DeidRsPipeline:
    """Context-free wrapper for the dicom-deid-rs CLI.

    Unlike CTPPipeline, this runs synchronously -- no polling loop needed.
    """

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        anonymizer_script: str | None = None,
        filter_script: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        quarantine_dir: str | None = None,
        binary_path: str | None = None,
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.anonymizer_script = anonymizer_script
        self.filter_script = filter_script
        self.deid_pixels = deid_pixels
        self.lookup_table = lookup_table
        self.quarantine_dir = quarantine_dir
        self.binary_path = binary_path or _get_default_binary_path()

    def run(self) -> ImageDeidLocalResult:
        """Run the de-identification pipeline.

        Translates CTP scripts to recipe format, invokes the Rust binary,
        and returns results in the same format as CTPPipeline.
        """
        # Load pixel script if needed
        pixel_script = None
        if self.deid_pixels:
            pixel_script = _get_default_pixel_script()
            if pixel_script is None:
                logging.warning(
                    "Pixel deid enabled but DicomPixelAnonymizer.script not found"
                )

        # Build recipe
        recipe_text, variables = build_recipe(
            anonymizer_xml=self.anonymizer_script,
            pixel_script=pixel_script,
            filter_script=self.filter_script,
        )

        # Write temp files
        temp_files = []
        try:
            recipe_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False
            )
            recipe_file.write(recipe_text)
            recipe_file.close()
            temp_files.append(recipe_file.name)

            # Build command
            cmd = [
                self.binary_path,
                self.input_dir,
                self.output_dir,
                recipe_file.name,
            ]

            # Add variables
            for name, value in variables.items():
                cmd.extend(["--var", name, value])

            # Add lookup table
            if self.lookup_table:
                lookup_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".properties", delete=False
                )
                lookup_file.write(self.lookup_table)
                lookup_file.close()
                temp_files.append(lookup_file.name)
                cmd.extend(["--lookup-table", lookup_file.name])

            logging.info(f"Running dicom-deid-rs: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=None,
            )

            if result.returncode != 0:
                logging.error(f"dicom-deid-rs stderr: {result.stderr}")
                raise RuntimeError(
                    f"dicom-deid-rs exited with code {result.returncode}: {result.stderr}"
                )

            # Parse output
            report = _parse_report(result.stdout)
            num_saved = report.get("files_processed", 0)
            num_quarantined = report.get("files_blacklisted", 0) + report.get(
                "files_skipped", 0
            )

            logging.info(f"dicom-deid-rs complete: {num_saved} processed, {num_quarantined} quarantined/skipped")

            return {
                "num_images_saved": num_saved,
                "num_images_quarantined": num_quarantined,
            }

        finally:
            for path in temp_files:
                try:
                    os.unlink(path)
                except OSError:
                    pass


_REPORT_RE = re.compile(r"Files (\w+):\s*(\d+)")


def _parse_report(stdout: str) -> dict[str, int]:
    """Parse the report numbers from dicom-deid-rs stdout."""
    report: dict[str, int] = {}
    for line in stdout.splitlines():
        m = _REPORT_RE.search(line)
        if m:
            key = m.group(1).lower()
            report[f"files_{key}"] = int(m.group(2))
    return report

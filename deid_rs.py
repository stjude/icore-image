"""Wrapper for the dicom-deid-rs Rust de-identification engine."""

import logging
import os
import re
import selectors
import subprocess
import sys
import tempfile
from typing import IO

from utils import ImageDeidLocalResult


def _get_default_binary_path() -> str:
    """Locate the dicom-deid-rs binary."""
    if getattr(sys, "frozen", False):
        candidates = [
            os.path.join(os.path.dirname(sys.executable), "dicom-deid-rs"),
        ]
        if hasattr(sys, "_MEIPASS"):
            candidates.append(os.path.join(sys._MEIPASS, "dicom-deid-rs"))
        for path in candidates:
            if os.path.exists(path):
                return path
        return candidates[0]

    return os.path.join(
        os.path.dirname(__file__), "dicom-deid-rs", "target", "release", "dicom-deid-rs"
    )


def _get_default_pixel_script_path() -> str | None:
    """Return the path to the default CTP pixel anonymizer script."""
    script_path = os.path.join(
        os.path.dirname(__file__), "ctp", "scripts", "DicomPixelAnonymizer.script"
    )
    if os.path.exists(script_path):
        return script_path
    return None


class DeidRsPipeline:
    """Context-free wrapper for the dicom-deid-rs CLI.

    Uses the built-in translate-ctp subcommand to translate CTP scripts
    to recipe format, then runs the de-identification pipeline.
    """

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        anonymizer_script: str | None = None,
        filter_script: str | None = None,
        sc_pdf_blacklist: str | None = None,
        deid_pixels: bool = False,
        lookup_table: str | None = None,
        quarantine_dir: str | None = None,
        binary_path: str | None = None,
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.anonymizer_script = anonymizer_script
        self.filter_script = filter_script
        self.sc_pdf_blacklist = sc_pdf_blacklist
        self.deid_pixels = deid_pixels
        self.lookup_table = lookup_table
        self.quarantine_dir = quarantine_dir
        self.binary_path = binary_path or _get_default_binary_path()

    def run(self) -> ImageDeidLocalResult:
        """Run the de-identification pipeline.

        Uses the Rust translate-ctp subcommand to translate CTP scripts,
        then invokes the pipeline with the generated recipe.
        """
        temp_files: list[str] = []
        try:
            # Step 1: Translate CTP scripts using the Rust translator
            recipe_path, variables, remove_private_tags, remove_unspecified = (
                self._translate_ctp_scripts(temp_files)
            )

            logging.info("=" * 80)
            logging.info("GENERATED RECIPE (via translate-ctp):")
            logging.info("=" * 80)
            with open(recipe_path, "r") as f:
                logging.info(f.read())
            logging.info("=" * 80)
            if variables:
                logging.info(f"Recipe variables: {variables}")
            logging.info(
                f"Config: remove_private_tags={remove_private_tags}, "
                f"remove_unspecified_elements={remove_unspecified}"
            )

            # Step 2: Build pipeline command
            cmd = [
                self.binary_path,
                self.input_dir,
                self.output_dir,
                recipe_path,
            ]

            for name, value in variables.items():
                cmd.extend(["--var", name, value])

            if not remove_private_tags:
                cmd.append("--keep-private-tags")

            if remove_unspecified:
                cmd.append("--remove-unspecified-elements")

            if self.lookup_table:
                lookup_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".properties", delete=False
                )
                lookup_file.write(self.lookup_table)
                lookup_file.close()
                temp_files.append(lookup_file.name)
                cmd.extend(["--lookup-table", lookup_file.name])

            logging.info(f"Running dicom-deid-rs: {' '.join(cmd)}")

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Stream stderr (progress updates) to logging in real time
            stdout_lines: list[str] = []
            stderr_lines: list[str] = []

            assert process.stdout is not None
            assert process.stderr is not None
            proc_stdout: IO[str] = process.stdout
            proc_stderr: IO[str] = process.stderr

            streams: dict[int, IO[str]] = {
                proc_stdout.fileno(): proc_stdout,
                proc_stderr.fileno(): proc_stderr,
            }

            sel = selectors.DefaultSelector()
            sel.register(proc_stdout, selectors.EVENT_READ)
            sel.register(proc_stderr, selectors.EVENT_READ)

            while streams:
                for key, _ in sel.select():
                    fd = key.fd
                    stream = streams[fd]
                    line = stream.readline()
                    if not line:
                        sel.unregister(key.fileobj)
                        del streams[fd]
                        continue
                    line = line.rstrip("\n")
                    if stream is proc_stderr:
                        stderr_lines.append(line)
                        logging.info(f"[dicom-deid-rs] {line}")
                    else:
                        stdout_lines.append(line)
                        logging.info(f"[dicom-deid-rs] {line}")

            sel.close()
            process.wait()

            stdout_text = "\n".join(stdout_lines)
            stderr_text = "\n".join(stderr_lines)

            if process.returncode != 0:
                raise RuntimeError(
                    f"dicom-deid-rs exited with code {process.returncode}: {stderr_text}"
                )

            # Check for blacklisted_files.txt
            blacklist_report = os.path.join(self.output_dir, "blacklisted_files.txt")
            if os.path.exists(blacklist_report):
                with open(blacklist_report, "r") as f:
                    content = f.read()
                logging.info("=" * 80)
                logging.info("BLACKLISTED FILES (rejected by filter):")
                logging.info("=" * 80)
                logging.info(content)
                logging.info("=" * 80)

            # Parse output
            report = _parse_report(stdout_text)
            num_saved = report.get("files_processed", 0)
            num_quarantined = report.get("files_blacklisted", 0) + report.get(
                "files_skipped", 0
            )

            logging.info(
                f"dicom-deid-rs complete: {num_saved} processed, {num_quarantined} quarantined/skipped"
            )

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

    def _translate_ctp_scripts(
        self, temp_files: list[str]
    ) -> tuple[str, dict[str, str], bool, bool]:
        """Use dicom-deid-rs translate-ctp to convert CTP scripts to recipe.

        Returns (recipe_path, variables, remove_private_tags, remove_unspecified_elements).
        """
        # Write anonymizer script to temp file
        anon_file = tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False)
        anon_file.write(self.anonymizer_script or "<script></script>")
        anon_file.close()
        temp_files.append(anon_file.name)

        # Recipe output file
        recipe_file = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        recipe_file.close()
        temp_files.append(recipe_file.name)

        # Build translate-ctp command
        cmd = [
            self.binary_path,
            "translate-ctp",
            anon_file.name,
            "-o",
            recipe_file.name,
        ]

        # Add pixel script
        if self.deid_pixels:
            pixel_path = _get_default_pixel_script_path()
            if pixel_path:
                cmd.extend(["--pixel", pixel_path])
            else:
                logging.warning(
                    "Pixel deid enabled but DicomPixelAnonymizer.script not found"
                )

        # Add filter script
        if self.filter_script:
            filter_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".script", delete=False
            )
            filter_file.write(self.filter_script)
            filter_file.close()
            temp_files.append(filter_file.name)
            cmd.extend(["--filter", filter_file.name])

        # Add blacklist script (e.g. SC/PDF exclusion)
        if self.sc_pdf_blacklist:
            blacklist_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".script", delete=False
            )
            blacklist_file.write(self.sc_pdf_blacklist)
            blacklist_file.close()
            temp_files.append(blacklist_file.name)
            cmd.extend(["--blacklist", blacklist_file.name])

        logging.info(f"Translating CTP scripts: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"translate-ctp failed with code {result.returncode}: {result.stderr}"
            )

        # Parse variables and config from stderr
        variables: dict[str, str] = {}
        remove_private_tags = True
        remove_unspecified_elements = False

        for line in result.stderr.splitlines():
            line = line.strip()
            if (
                "=" in line
                and not line.startswith("Config:")
                and not line.startswith("Variables:")
            ):
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if key and val and not key.startswith("remove_"):
                    variables[key] = val
            if line.startswith("remove_private_tags:"):
                remove_private_tags = "true" in line
            if line.startswith("remove_unspecified_elements:"):
                remove_unspecified_elements = "true" in line

        return recipe_file.name, variables, remove_private_tags, remove_unspecified_elements


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

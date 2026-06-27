from pipeline.base import PipelineStage
from pipeline.context import PipelineContext
from pipeline.header_extract import headerextract_local


class HeaderExtractStage(PipelineStage):
    """Stage: extract DICOM headers from the de-identified output into metadata.xlsx.

    Runs after de-identification, reading the de-identified ``.dcm`` files in
    ``ctx.output_dir`` and writing ``metadata.xlsx`` back into the same directory
    so it ships to Azure alongside the de-identified data.
    """

    progress_marker = ("header_extract", "Extracting headers")

    def __init__(
        self,
        headers_to_extract: list[str] | None = None,
        extract_all_headers: bool = False,
    ) -> None:
        self.headers_to_extract = headers_to_extract
        self.extract_all_headers = extract_all_headers

    def execute(self, ctx: PipelineContext) -> None:
        def on_file(done: int, total: int) -> None:
            if ctx.progress and total:
                ctx.progress.update(
                    "header_extract",
                    done / total,
                    f"Extracting headers from {done} of {total} files",
                )

        result = headerextract_local(
            input_dir=ctx.output_dir,
            output_dir=ctx.output_dir,
            headers_to_extract=self.headers_to_extract,
            extract_all_headers=self.extract_all_headers,
            debug=ctx.debug,
            run_dirs=ctx.run_dirs,
            progress_callback=on_file,
        )

        ctx.header_files_processed = result["num_files_processed"]
        ctx.header_studies = result["num_studies"]

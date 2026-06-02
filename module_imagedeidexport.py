from config import IcoreConfig
from pipeline import ImageDeidExportPipeline
from utils import DeidExportResult, RunDirs, Spreadsheet


def imagedeidexport(
    config: IcoreConfig,
    *,
    query_spreadsheet: Spreadsheet,
    output_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> DeidExportResult:
    return ImageDeidExportPipeline(
        config,
        query_spreadsheet=query_spreadsheet,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

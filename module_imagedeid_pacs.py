from config import IcoreConfig
from pipeline import ImageDeidPacsPipeline

# Re-exported for backwards compatibility with ``icore_processor.py``.
from pipeline.stages.image_deid import _collect_engine_audit_files  # noqa: F401
from utils import PacsQueryResult, RunDirs, Spreadsheet


def imagedeid_pacs(
    config: IcoreConfig,
    *,
    query_spreadsheet: Spreadsheet,
    output_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> PacsQueryResult:
    return ImageDeidPacsPipeline(
        config,
        query_spreadsheet=query_spreadsheet,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

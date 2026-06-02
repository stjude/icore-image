from config import IcoreConfig
from pipeline import ImageDeidLocalPipeline

# Private helpers re-exported here for backwards compatibility with existing
# tests (e.g. test_module_imagedeid_local.py) and ``icore_processor.py``.
from pipeline.stages.image_deid import (  # noqa: F401
    _apply_default_filter_script,
    _collect_engine_audit_files,
    _combine_with_sc_pdf,
    _generate_lookup_table_content,
    _get_sc_pdf_blacklist,
    _process_mapping_file,
)
from utils import ImageDeidLocalResult, RunDirs


def imagedeid_local(
    config: IcoreConfig,
    *,
    input_dir: str,
    output_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> ImageDeidLocalResult:
    return ImageDeidLocalPipeline(
        config,
        input_dir=input_dir,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

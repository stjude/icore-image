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
from utils import ImageDeidLocalResult, RunDirs, DeidEngine


def imagedeid_local(
    input_dir: str,
    output_dir: str,
    appdata_dir: str | None = None,
    filter_script: str | None = None,
    anonymizer_script: str | None = None,
    deid_pixels: bool = False,
    lookup_table: str | None = None,
    debug: bool = False,
    run_dirs: RunDirs | None = None,
    apply_default_filter_script: bool = True,
    mapping_file_path: str | None = None,
    sc_pdf_output_dir: str | None = None,
    deid_engine: DeidEngine = "ctp",
) -> ImageDeidLocalResult:
    return ImageDeidLocalPipeline(
        input_dir=input_dir,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        filter_script=filter_script,
        anonymizer_script=anonymizer_script,
        deid_pixels=deid_pixels,
        lookup_table=lookup_table,
        debug=debug,
        run_dirs=run_dirs,
        apply_default_filter_script=apply_default_filter_script,
        mapping_file_path=mapping_file_path,
        sc_pdf_output_dir=sc_pdf_output_dir,
        deid_engine=deid_engine,
    ).run()

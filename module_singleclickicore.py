from config import IcoreConfig
from pipeline import SingleClickIcorePipeline
from utils import RunDirs, SingleClickResult, Spreadsheet


def singleclickicore(
    config: IcoreConfig,
    *,
    query_spreadsheet: Spreadsheet,
    input_file: str,
    output_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> SingleClickResult:
    return SingleClickIcorePipeline(
        config,
        query_spreadsheet=query_spreadsheet,
        input_file=input_file,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

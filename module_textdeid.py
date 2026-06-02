from config import IcoreConfig
from pipeline import TextDeidPipeline
from utils import RunDirs, TextDeidResult


def textdeid(
    config: IcoreConfig,
    *,
    input_file: str,
    output_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> TextDeidResult:
    return TextDeidPipeline(
        config,
        input_file=input_file,
        output_dir=output_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

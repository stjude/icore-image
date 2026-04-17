from pipeline import TextDeidPipeline
from utils import RunDirs, TextDeidResult


def textdeid(
    input_file: str,
    output_dir: str,
    to_keep_list: list[str] | None = None,
    to_remove_list: list[str] | None = None,
    columns_to_drop: list[str] | None = None,
    columns_to_deid: list[str] | None = None,
    debug: bool = False,
    run_dirs: RunDirs | None = None,
) -> TextDeidResult:
    return TextDeidPipeline(
        input_file=input_file,
        output_dir=output_dir,
        to_keep_list=to_keep_list,
        to_remove_list=to_remove_list,
        columns_to_drop=columns_to_drop,
        columns_to_deid=columns_to_deid,
        debug=debug,
        run_dirs=run_dirs,
    ).run()

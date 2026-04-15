from pipeline import ImageExportPipeline
from utils import RunDirs


def image_export(
    input_dir: str,
    sas_url: str,
    project_name: str,
    appdata_dir: str | None = None,
    debug: bool = False,
    run_dirs: RunDirs | None = None,
) -> dict[str, str]:
    return ImageExportPipeline(
        input_dir=input_dir,
        sas_url=sas_url,
        project_name=project_name,
        appdata_dir=appdata_dir,
        debug=debug,
        run_dirs=run_dirs,
    ).run()

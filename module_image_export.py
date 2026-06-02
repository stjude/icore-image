from config import IcoreConfig
from pipeline import ImageExportPipeline
from utils import RunDirs


def image_export(
    config: IcoreConfig,
    *,
    input_dir: str,
    appdata_dir: str | None = None,
    run_dirs: RunDirs | None = None,
) -> dict[str, str]:
    return ImageExportPipeline(
        config,
        input_dir=input_dir,
        appdata_dir=appdata_dir,
        run_dirs=run_dirs,
    ).run()

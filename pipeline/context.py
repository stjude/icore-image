from dataclasses import dataclass, field

from utils import RunDirs


@dataclass
class PipelineContext:
    """Mutable state passed through every stage of a Pipeline.

    Stages read their inputs and write their outputs on this object. Fields
    are logically grouped by the stage that produces them; later stages read
    from slots earlier stages populated.
    """

    # Ambient configuration — populated before any stage runs
    run_dirs: RunDirs
    output_dir: str
    appdata_dir: str
    debug: bool = False

    # Stage-1 (Gather) outputs
    dicom_input_dir: str | None = None
    total_files: int | None = None
    gathered_studies: int = 0
    failed_query_indices: list[int] = field(default_factory=list)

    # When the Gather stage is PACS-based, it merges the query-derived
    # filter into the user's filter script and stores the result here for
    # the downstream image-deid stage to consume.
    gather_filter_override: str | None = None

    # Stage-2 (Image de-id) outputs
    images_saved: int = 0
    images_quarantined: int = 0

    # Stage-3 (Text de-id) input + outputs
    text_input_file: str | None = None
    text_output_file: str | None = None
    text_rows_processed: int = 0

    # Stage-4 (Export) outputs
    export_performed: bool = False

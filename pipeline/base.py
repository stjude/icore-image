from abc import ABC, abstractmethod
from typing import Any

from pipeline.context import PipelineContext
from pipeline.progress import ProgressReporter


class PipelineStage(ABC):
    """A single stage in a de-id pipeline.

    Stages mutate a shared ``PipelineContext`` rather than returning values,
    so later stages can read the outputs of earlier ones.
    """

    # ``(stage_key, label)`` shown on the task-progress bar, or ``None`` for
    # stages that contribute no marker (e.g. instant local file gather, export
    # which is folded into the terminal "Ready for QC" point).
    progress_marker: tuple[str, str] | None = None

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> None: ...


class Pipeline(ABC):
    """Declarative 5-stage de-id pipeline.

    The five stages run in fixed order: Gather, ImageDeid, TextDeid,
    HeaderExtract, Export. Subclasses override the ``build_*_stage`` factory
    methods for whichever stages they need; any stage that returns ``None``
    from its factory is skipped.

    Subclasses must also override :meth:`_build_context` (to seed ambient
    config and any stage inputs that are not stage outputs, e.g. the text
    input file) and :meth:`_to_result` (to project the final context onto
    the module's historical ``TypedDict`` result shape).
    """

    # --- stage factories (override in subclasses) ---

    def build_gather_stage(self) -> PipelineStage | None:
        return None

    def build_image_deid_stage(self) -> PipelineStage | None:
        return None

    def build_text_deid_stage(self) -> PipelineStage | None:
        return None

    def build_header_extract_stage(self) -> PipelineStage | None:
        return None

    def build_export_stage(self) -> PipelineStage | None:
        return None

    # --- context / result projection (override in subclasses) ---

    @abstractmethod
    def _build_context(self) -> PipelineContext: ...

    @abstractmethod
    def _to_result(self, ctx: PipelineContext) -> Any: ...

    # --- main entry point ---

    def run(self) -> Any:
        ctx = self._build_context()
        stages: list[PipelineStage | None] = [
            self.build_gather_stage(),
            self.build_image_deid_stage(),
            self.build_text_deid_stage(),
            self.build_header_extract_stage(),
            self.build_export_stage(),
        ]
        markers = [
            s.progress_marker
            for s in stages
            if s is not None and s.progress_marker is not None
        ]
        ctx.progress = ProgressReporter(ctx.run_dirs["log_dir"], markers)
        try:
            for stage in stages:
                if stage is not None:
                    stage.execute(ctx)
            return self._to_result(ctx)
        finally:
            for stage in stages:
                if stage is None:
                    continue
                cleanup = getattr(stage, "cleanup", None)
                if callable(cleanup):
                    cleanup(ctx)

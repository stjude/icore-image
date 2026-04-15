from abc import ABC, abstractmethod
from typing import Any

from pipeline.context import PipelineContext


class PipelineStage(ABC):
    """A single stage in a de-id pipeline.

    Stages mutate a shared ``PipelineContext`` rather than returning values,
    so later stages can read the outputs of earlier ones.
    """

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> None: ...


class Pipeline(ABC):
    """Declarative 4-stage de-id pipeline.

    The four stages run in fixed order: Gather, ImageDeid, TextDeid, Export.
    Subclasses override the ``build_*_stage`` factory methods for whichever
    stages they need; any stage that returns ``None`` from its factory is
    skipped.

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
            self.build_export_stage(),
        ]
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

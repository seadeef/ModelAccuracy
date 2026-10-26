from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from atmospheric_rivers import discover_target_ar_sources
from boundary_paths import WBD_LAYERS_DIR
from model_registry import MODEL_REGISTRY
from stats_pipeline.episode_metrics import (
    BASIN_HISTORY_FILENAME,
    CALIFORNIA_HUC2,
    basin_history_filename,
)
from stats_pipeline.basin import run_basin_stage
from stats_pipeline.consistency import run_consistency_stage
from stats_pipeline.cube import run_cube_stage
from stats_pipeline.episodes import (
    DEFAULT_MIN_AREA_FRACTION,
    find_basin_episodes,
    run_compare_stage,
    run_episode_stage,
)
from stats_pipeline.manifest import (
    digest_files_content,
    fingerprint_files,
    load_manifest,
    stage_state,
    update_stage,
)
from stats_pipeline.verification import run_verification_stage

StageName = Literal["verification", "consistency", "basin", "episodes", "cube", "compare"]
STAGE_ORDER: tuple[StageName, ...] = (
    "verification", "consistency", "basin", "episodes", "cube", "compare")


@dataclass(frozen=True)
class StageResult:
    model: str
    stage: StageName
    status: Literal["completed", "skipped"]
    message: str


@dataclass
class RunSummary:
    results: list[StageResult] = field(default_factory=list)

    def add(self, model: str, stage: StageName, status: str, message: str) -> None:
        self.results.append(StageResult(model, stage, status, message))

    @property
    def skipped(self) -> list[StageResult]:
        return [result for result in self.results if result.status == "skipped"]


def _wbd_available() -> bool:
    return (WBD_LAYERS_DIR / "hu6.json").is_file()


def _basin_input_fingerprint(model: str) -> str:
    data_root = Path(MODEL_REGISTRY[model].data_dir)
    return fingerprint_files([
        WBD_LAYERS_DIR / "hu6.json",
        data_root / "grid_lats.npy",
        data_root / "grid_lons.npy",
    ])


def _record_basin(model: str, output: Path, input_fingerprint: str) -> None:
    root = Path("stats_output") / model
    previous = stage_state(root, "basin") or {}
    fingerprint = fingerprint_files([output])
    update_stage(
        root,
        "basin",
        {"fingerprint": fingerprint, "input_fingerprint": input_fingerprint,
         "output": output.name, "algorithm": "basin-v2"},
        invalidate=("episodes",) if (
            previous.get("fingerprint") != fingerprint
            or previous.get("input_fingerprint") != input_fingerprint
        ) else (),
    )


def _record_episodes(model: str, output: Path, sources: list[Path]) -> None:
    root = Path("stats_output") / model
    update_stage(root, "episodes", {
        "target_digest": digest_files_content(sources),
        "basin_history_digest": digest_files_content([root / BASIN_HISTORY_FILENAME]),
        "parameters": {"minimum_basin_area_fraction": DEFAULT_MIN_AREA_FRACTION},
        "algorithm": "episodes-v2",
        "output": output.name,
    })


def run_pipeline(
    *,
    models: list[str] | None = None,
    only: set[StageName] | None = None,
    force: bool = False,
) -> RunSummary:
    selected_models = models or list(MODEL_REGISTRY)
    requested = set(only or STAGE_ORDER)
    # Episodes structurally depend on basin histories. A targeted episode refresh
    # therefore resolves basin first, while reporting it as a dependency stage.
    execution = set(requested)
    if "episodes" in requested:
        execution.add("basin")
    sources = discover_target_ar_sources()
    summary = RunSummary()
    episode_catalog = None

    for model in selected_models:
        root = Path("stats_output") / model
        load_manifest(root, migrate=True)
        basin_ready = True
        for stage in STAGE_ORDER:
            if stage not in execution or stage == "compare":
                continue
            if stage == "verification":
                run_verification_stage(model, force=force)
                summary.add(model, stage, "completed", "verification outputs current")
            elif stage == "consistency":
                run_consistency_stage(model, force=force)
                summary.add(model, stage, "completed", "coupled consistency outputs current")
            elif stage == "basin":
                if not _wbd_available():
                    basin_ready = False
                    summary.add(model, stage, "skipped", "missing WBD HUC6 boundaries")
                    continue
                basin_inputs = _basin_input_fingerprint(model)
                previous_basin = stage_state(root, "basin") or {}
                basin_force = force or previous_basin.get("input_fingerprint") != basin_inputs
                output = run_basin_stage(model, force=basin_force)
                _record_basin(model, output, basin_inputs)
                summary.add(model, stage, "completed", "basin history current")
            elif stage == "episodes":
                if not basin_ready:
                    summary.add(model, stage, "skipped", "basin prerequisite unavailable")
                    continue
                if not sources:
                    summary.add(model, stage, "skipped", "missing staged tARget data; run download.py --ar")
                    continue
                if episode_catalog is None:
                    episode_catalog = find_basin_episodes(sources)
                output = run_episode_stage(model, episode_catalog, force=force)
                _record_episodes(model, output, sources)
                summary.add(model, stage, "completed", "episode catalog and scoped statistics current")
            elif stage == "cube":
                # Per-cell daily cube for on-demand regional series. Self-gated
                # on its meta.json (dates + lead range), not the manifest: it
                # depends only on the raw archive and PRISM, like the basin stage.
                run_cube_stage(model, force=force)
                summary.add(model, stage, "completed", "per-cell daily cube current")

    # Cross-model ΔR is a single global pass, not a per-model stage. It is
    # cheap (JSON + one basin-history read per model), so it recomputes every
    # run without manifest gating and skips gracefully when fewer than two
    # models carry v3 episode statistics.
    if "compare" in execution:
        output = run_compare_stage(selected_models)
        if output is None:
            summary.add("all", "compare", "skipped",
                        "fewer than two models with v3 episode statistics")
        else:
            summary.add("all", "compare", "completed",
                        "cross-model episode comparison current")
    return summary


def run_region_pipeline(
    huc2: str,
    *,
    models: list[str] | None = None,
    only: set[StageName] | None = None,
    force: bool = False,
    include_delta_j: bool = True,
) -> RunSummary:
    """basin → episodes → compare for one non-California HUC2 region (W3).

    The California path (``run_pipeline``) is untouched: it stays
    manifest-gated and writes the frozen artifact names. Region runs write
    region-scoped artifacts (``basin_consistency_huc6_{huc2}.npz``,
    ``episodes_huc{huc2}.json``,
    ``episode_model_comparison_huc{huc2}.json``) and are not manifest-gated —
    the manifest's stage keys describe the CA products.

    Every estimand, cluster rule, seed policy and version window is the
    California machinery verbatim.
    """
    if huc2 == CALIFORNIA_HUC2:
        raise ValueError(
            "use run_pipeline() for California (HUC2 18) — it is manifest-gated "
            "and owns the frozen artifact names")
    selected_models = models or list(MODEL_REGISTRY)
    requested = set(only or ("basin", "episodes", "compare"))
    execution = set(requested)
    if "episodes" in requested:
        execution.add("basin")
    sources = discover_target_ar_sources()
    summary = RunSummary()
    episode_catalog = None

    for model in selected_models:
        if "basin" in execution:
            if not _wbd_available():
                summary.add(model, "basin", "skipped", "missing WBD HUC6 boundaries")
                continue
            run_basin_stage(model, force=force, huc2=huc2)
            summary.add(model, "basin", "completed",
                        f"basin history current (HUC2 {huc2})")
        if "episodes" in execution:
            if not sources:
                summary.add(model, "episodes", "skipped",
                            "missing staged tARget data; run download.py --ar")
                continue
            if episode_catalog is None:
                episode_catalog = find_basin_episodes(sources, huc2=huc2)
            run_episode_stage(model, episode_catalog, force=force, huc2=huc2)
            summary.add(model, "episodes", "completed",
                        f"episode catalog and scoped statistics current (HUC2 {huc2})")

    if "compare" in execution:
        output = run_compare_stage(selected_models, huc2=huc2,
                                   include_delta_j=include_delta_j)
        if output is None:
            summary.add("all", "compare", "skipped",
                        "fewer than two models with region episode statistics")
        else:
            summary.add("all", "compare", "completed",
                        f"cross-model comparison current (HUC2 {huc2})")
    return summary

from __future__ import annotations

from copy import deepcopy
from time import perf_counter
from typing import Any
import warnings

import pandas as pd

from caitsith_studio.core.operation_registry import OperationRegistry
from caitsith_studio.core.pipeline import sort_steps, summarize_preview, validate_step
from caitsith_studio.models import DataFrameRegistry, ExecutionLog, PipelineResult, PipelineStep, PreviewArtifact, StepExecutionDetail


class PipelineExecutor:
    def __init__(self, registry: OperationRegistry):
        self.registry = registry

    def execute(
        self,
        dataframe_registry: DataFrameRegistry,
        steps: list[PipelineStep],
        *,
        base_frames: dict[str, pd.DataFrame] | None = None,
        start_at_step_id: str | None = None,
        stop_after_step_id: str | None = None,
    ) -> PipelineResult:
        working_frames = deepcopy(base_frames) if base_frames is not None else dataframe_registry.copy_frames()
        ordered_steps = sort_steps(steps)
        logs: list[ExecutionLog] = []
        snapshots: dict[str, dict[str, pd.DataFrame]] = {}
        details: dict[str, StepExecutionDetail] = {}

        started = start_at_step_id is None
        for step in ordered_steps:
            if not started:
                if step.id == start_at_step_id:
                    started = True
                else:
                    continue

            if not step.enabled:
                logs.append(
                    ExecutionLog.create(
                        step_id=step.id,
                        step_order=step.step_order,
                        df_name=step.df_name,
                        formula=step.formula,
                        status="disabled",
                        message="Paso desactivado; no se ejecuta.",
                    )
                )
                snapshots[step.id] = _copy_frames(working_frames)
                if step.id == stop_after_step_id:
                    break
                continue

            spec = self.registry.get(step.formula)
            errors, warnings_found = validate_step(step, spec, working_frames)
            if errors:
                error_message = " | ".join(errors)
                logs.append(
                    ExecutionLog.create(
                        step_id=step.id,
                        step_order=step.step_order,
                        df_name=step.df_name,
                        formula=step.formula,
                        status="error",
                        message=error_message,
                        warning=" | ".join(warnings_found) if warnings_found else None,
                    )
                )
                details[step.id] = StepExecutionDetail(
                    step_id=step.id,
                    df_name=step.df_name,
                    status="error",
                    message=error_message,
                    warning=" | ".join(warnings_found) if warnings_found else None,
                    before_shape=(0, 0),
                    after_shape=(0, 0),
                )
                break

            before_df = working_frames[step.df_name].copy()
            before_shape = tuple(before_df.shape)
            resolved_params = self._resolve_call_parameters(step.parameters, spec, working_frames)

            start_time = perf_counter()
            try:
                with warnings.catch_warnings(record=True) as captured_warnings:
                    warnings.simplefilter("always")
                    runtime = self.registry.caitsith_class(before_df.copy())
                    method = getattr(runtime, step.formula)
                    result = method(**resolved_params)
                duration_ms = (perf_counter() - start_time) * 1000
                warning_text = _warning_text(captured_warnings, extra_warnings=warnings_found)

                after_df = self._resolve_after_frame(runtime=runtime, before_df=before_df, result=result)
                working_frames[step.df_name] = after_df.copy()
                detail = _build_step_detail(
                    step=step,
                    before_df=before_df,
                    after_df=after_df,
                    warning_text=warning_text,
                    return_value=result,
                )
                details[step.id] = detail
                logs.append(
                    ExecutionLog.create(
                        step_id=step.id,
                        step_order=step.step_order,
                        df_name=step.df_name,
                        formula=step.formula,
                        status="executed",
                        message=detail.message,
                        warning=warning_text,
                        duration_ms=duration_ms,
                    )
                )
                snapshots[step.id] = _copy_frames(working_frames)
            except Exception as error:
                duration_ms = (perf_counter() - start_time) * 1000
                error_message = str(error)
                logs.append(
                    ExecutionLog.create(
                        step_id=step.id,
                        step_order=step.step_order,
                        df_name=step.df_name,
                        formula=step.formula,
                        status="error",
                        message=error_message,
                        warning=" | ".join(warnings_found) if warnings_found else None,
                        duration_ms=duration_ms,
                    )
                )
                details[step.id] = StepExecutionDetail(
                    step_id=step.id,
                    df_name=step.df_name,
                    status="error",
                    message=error_message,
                    warning=" | ".join(warnings_found) if warnings_found else None,
                    before_shape=before_shape,
                    after_shape=before_shape,
                )
                break

            if step.id == stop_after_step_id:
                break

        return PipelineResult(frames=working_frames, snapshots=snapshots, logs=logs, step_details=details)

    def preview_step(
        self,
        dataframe_registry: DataFrameRegistry,
        steps: list[PipelineStep],
        selected_step_id: str,
        *,
        base_frames: dict[str, pd.DataFrame] | None = None,
        start_at_step_id: str | None = None,
    ) -> PreviewArtifact:
        working_frames = deepcopy(base_frames) if base_frames is not None else dataframe_registry.copy_frames()
        started = start_at_step_id is None

        for step in sort_steps(steps):
            if not started:
                if step.id == start_at_step_id:
                    started = True
                else:
                    continue

            if not step.enabled:
                if step.id == selected_step_id:
                    spec = self.registry.get(step.formula)
                    current_df = working_frames.get(step.df_name, pd.DataFrame())
                    return summarize_preview(
                        step=step,
                        spec=spec,
                        before_df=current_df,
                        after_df=current_df,
                        validation_warnings=["El paso esta desactivado."],
                    )
                continue

            spec = self.registry.get(step.formula)
            errors, warnings_found = validate_step(step, spec, working_frames)
            before_df = working_frames[step.df_name].copy() if step.df_name in working_frames else pd.DataFrame()

            if step.id == selected_step_id:
                if errors:
                    return summarize_preview(
                        step=step,
                        spec=spec,
                        before_df=before_df,
                        after_df=before_df,
                        validation_errors=errors,
                        validation_warnings=warnings_found,
                    )
                runtime = self.registry.caitsith_class(before_df.copy())
                method = getattr(runtime, step.formula)
                result = method(**self._resolve_call_parameters(step.parameters, spec, working_frames))
                after_df = self._resolve_after_frame(runtime=runtime, before_df=before_df, result=result)
                return summarize_preview(
                    step=step,
                    spec=spec,
                    before_df=before_df,
                    after_df=after_df,
                    validation_warnings=warnings_found,
                )

            if errors:
                break

            runtime = self.registry.caitsith_class(before_df.copy())
            method = getattr(runtime, step.formula)
            result = method(**self._resolve_call_parameters(step.parameters, spec, working_frames))
            working_frames[step.df_name] = self._resolve_after_frame(runtime=runtime, before_df=before_df, result=result)

        raise ValueError(f"No se pudo previsualizar el paso '{selected_step_id}'.")

    def _resolve_call_parameters(
        self,
        parameters: dict[str, Any],
        spec: Any,
        frames: dict[str, pd.DataFrame],
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        allowed_names = {parameter.name for parameter in spec.parameters}
        for key, value in parameters.items():
            if key not in allowed_names:
                continue
            if key == "external_df" and isinstance(value, str) and value in frames:
                resolved[key] = frames[value].copy()
            else:
                resolved[key] = value
        return resolved

    def _resolve_after_frame(self, *, runtime: Any, before_df: pd.DataFrame, result: Any) -> pd.DataFrame:
        if isinstance(result, pd.DataFrame):
            return result.copy()
        runtime_df = getattr(runtime, "df", before_df)
        if isinstance(runtime_df, pd.DataFrame):
            return runtime_df.copy()
        return before_df.copy()


def _copy_frames(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {name: frame.copy() for name, frame in frames.items()}


def _warning_text(captured_warnings: list[Any], extra_warnings: list[str] | None = None) -> str | None:
    messages = [str(warning.message) for warning in captured_warnings]
    if extra_warnings:
        messages.extend(extra_warnings)
    return " | ".join(message for message in messages if message) or None


def _build_step_detail(
    *,
    step: PipelineStep,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    warning_text: str | None,
    return_value: Any,
) -> StepExecutionDetail:
    new_columns = [column for column in after_df.columns if column not in before_df.columns]
    removed_columns = [column for column in before_df.columns if column not in after_df.columns]
    common_columns = [column for column in after_df.columns if column in before_df.columns]
    changed_columns = [column for column in common_columns if not before_df[column].equals(after_df[column])]

    if new_columns:
        message = f"Columnas nuevas: {', '.join(new_columns[:4])}"
    elif removed_columns:
        message = f"Columnas eliminadas: {', '.join(removed_columns[:4])}"
    elif before_df.shape[0] != after_df.shape[0]:
        message = f"Filas: {before_df.shape[0]} -> {after_df.shape[0]}"
    elif changed_columns:
        message = f"Columnas modificadas: {', '.join(changed_columns[:4])}"
    elif return_value is not None and not isinstance(return_value, pd.DataFrame):
        message = "Operacion agregada ejecutada correctamente."
    else:
        message = "Operacion ejecutada sin cambios estructurales visibles."

    return StepExecutionDetail(
        step_id=step.id,
        df_name=step.df_name,
        status="executed",
        message=message,
        warning=warning_text,
        before_shape=tuple(before_df.shape),
        after_shape=tuple(after_df.shape),
        new_columns=new_columns,
        removed_columns=removed_columns,
        changed_columns=changed_columns,
        return_value_repr=None if isinstance(return_value, pd.DataFrame) else repr(return_value)[:240],
    )
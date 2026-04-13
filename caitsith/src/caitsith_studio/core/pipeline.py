from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import uuid4

import pandas as pd

from caitsith_studio.models import OperationSpec, PipelineStep, PreviewArtifact


def new_pipeline_step(*, step_order: int, df_name: str = "", formula: str = "") -> PipelineStep:
    return PipelineStep(
        id=f"step-{uuid4().hex[:8]}",
        enabled=True,
        step_order=step_order,
        df_name=df_name,
        formula=formula,
    )


def clone_pipeline_step(step: PipelineStep, *, step_order: int | None = None) -> PipelineStep:
    cloned = deepcopy(step)
    cloned.id = f"step-{uuid4().hex[:8]}"
    cloned.step_order = step.step_order if step_order is None else step_order
    cloned.status = "pending"
    cloned.last_error = None
    cloned.last_warning = None
    return cloned


def sort_steps(steps: list[PipelineStep]) -> list[PipelineStep]:
    return sorted(steps, key=lambda step: (step.step_order, step.id))


def pipeline_table_frame(steps: list[PipelineStep]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for step in sort_steps(steps):
        rows.append(
            {
                "id": step.id,
                "enabled": step.enabled,
                "step_order": step.step_order,
                "df_name": step.df_name,
                "formula": step.formula,
                "target": step.target_label(),
                "status": step.status,
                "preview": step.preview,
                "notes": step.notes,
            }
        )
    return pd.DataFrame(rows)


def update_steps_from_editor(steps: list[PipelineStep], editor_frame: pd.DataFrame) -> list[PipelineStep]:
    by_id = {step.id: step for step in steps}
    updated_steps: list[PipelineStep] = []
    for row in editor_frame.to_dict(orient="records"):
        step = deepcopy(by_id[row["id"]])
        step.enabled = bool(row["enabled"])
        step.step_order = int(row["step_order"])
        step.df_name = str(row["df_name"] or "")
        step.formula = str(row["formula"] or "")
        step.notes = str(row.get("notes") or "")
        updated_steps.append(step)
    return sort_steps(updated_steps)


def summarize_preview(
    *,
    step: PipelineStep,
    spec: OperationSpec,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    validation_errors: list[str] | None = None,
    validation_warnings: list[str] | None = None,
) -> PreviewArtifact:
    new_columns = [column for column in after_df.columns if column not in before_df.columns]
    removed_columns = [column for column in before_df.columns if column not in after_df.columns]
    common_columns = [column for column in after_df.columns if column in before_df.columns]
    changed_columns = [column for column in common_columns if not before_df[column].equals(after_df[column])]
    row_delta = int(after_df.shape[0] - before_df.shape[0])

    if validation_errors:
        summary = "Faltan parametros o referencias validas para previsualizar el paso."
    elif spec.returns_dataframe and spec.kind == "row_filter":
        summary = f"Se transformaran las filas de {before_df.shape[0]} a {after_df.shape[0]}."
    elif new_columns:
        summary = f"Se crearan {len(new_columns)} columnas nuevas: {', '.join(new_columns[:4])}."
    elif removed_columns:
        summary = f"Se eliminaran columnas: {', '.join(removed_columns[:4])}."
    elif changed_columns:
        summary = f"Se modificaran {len(changed_columns)} columnas existentes."
    elif row_delta != 0:
        summary = f"El numero de filas cambiara en {row_delta:+d}."
    elif spec.kind == "sort":
        summary = "Se reordenara el DataFrame sin cambiar columnas."
    else:
        summary = "La operacion no altera la estructura visible del DataFrame, o devuelve un valor agregado."

    return PreviewArtifact(
        step_id=step.id,
        df_name=step.df_name,
        summary=summary,
        validation_errors=validation_errors or [],
        validation_warnings=validation_warnings or [],
        new_columns=new_columns,
        removed_columns=removed_columns,
        changed_columns=changed_columns,
        row_delta=row_delta,
        before_head=before_df.head(8).copy(),
        after_head=after_df.head(8).copy(),
    )


def validate_step(step: PipelineStep, spec: OperationSpec, frames: dict[str, pd.DataFrame]) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if not step.df_name:
        errors.append("Debes seleccionar un DataFrame para el paso.")
        return errors, warnings

    if step.df_name not in frames:
        errors.append(f"El DataFrame '{step.df_name}' no esta cargado.")
        return errors, warnings

    active_df = frames[step.df_name]
    active_columns = set(active_df.columns)
    external_df_name = step.parameters.get("external_df")
    external_columns = set()
    if isinstance(external_df_name, str) and external_df_name in frames:
        external_columns = set(frames[external_df_name].columns)

    for parameter in spec.parameters:
        value = step.parameters.get(parameter.name)
        if parameter.required and is_missing_parameter(value):
            errors.append(f"Falta el parametro obligatorio '{parameter.name}'.")
            continue

        if is_missing_parameter(value):
            continue

        if parameter.widget == "column":
            target_columns = active_columns
            target_name = step.df_name
            if parameter.name in {"lookup_column", "return_column"} and external_columns:
                target_columns = external_columns
                target_name = str(external_df_name)
            if str(value) not in target_columns:
                errors.append(f"La columna '{value}' no existe en '{target_name}'.")

        elif parameter.widget == "columns":
            missing = [column for column in value if column not in active_columns]
            if missing:
                errors.append(f"Faltan columnas en '{step.df_name}': {missing}")

        elif parameter.widget == "dataframe":
            if str(value) not in frames:
                errors.append(f"El DataFrame externo '{value}' no esta cargado.")

        elif parameter.widget == "conditions":
            for condition in value:
                if len(condition) < 3:
                    errors.append("Cada condicion debe tener columna, operador y valor.")
                    continue
                if str(condition[0]) not in active_columns:
                    errors.append(f"La columna '{condition[0]}' no existe en '{step.df_name}'.")

        elif parameter.widget == "condition_groups":
            if not value:
                warnings.append("No se han definido grupos de condiciones todavia.")
            for group in value:
                for condition in group:
                    if len(condition) < 3:
                        errors.append("Cada condicion agrupada debe tener columna, operador y valor.")
                        continue
                    if str(condition[0]) not in active_columns:
                        errors.append(f"La columna '{condition[0]}' no existe en '{step.df_name}'.")

    return errors, warnings


def is_missing_parameter(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False
from __future__ import annotations

from copy import deepcopy

import streamlit as st

from caitsith_studio.core.pipeline import clone_pipeline_step, new_pipeline_step, pipeline_table_frame, sort_steps, update_steps_from_editor
from caitsith_studio.models import PipelineStep


def render_pipeline_editor(
    *,
    steps: list[PipelineStep],
    df_names: list[str],
    formula_names: list[str],
    active_df_name: str | None,
    selected_step_id: str | None,
    key_prefix: str = "pipeline-editor",
) -> tuple[list[PipelineStep], str | None]:
    st.subheader("Pipeline de transformaciones")

    if not steps:
        default_formula = formula_names[0] if formula_names else ""
        default_df = active_df_name or (df_names[0] if df_names else "")
        steps = [new_pipeline_step(step_order=1, df_name=default_df, formula=default_formula)]

    editor_frame = pipeline_table_frame(steps)
    edited_frame = st.data_editor(
        editor_frame,
        hide_index=True,
        key=f"{key_prefix}-table",
        column_order=["enabled", "step_order", "df_name", "formula", "target", "status", "preview", "notes"],
        column_config={
            "enabled": st.column_config.CheckboxColumn("Activo"),
            "step_order": st.column_config.NumberColumn("Orden", min_value=1, step=1),
            "df_name": st.column_config.SelectboxColumn("DataFrame", options=df_names),
            "formula": st.column_config.SelectboxColumn("Formula", options=formula_names),
            "target": st.column_config.TextColumn("Target / salida", disabled=True),
            "status": st.column_config.TextColumn("Estado", disabled=True),
            "preview": st.column_config.TextColumn("Preview", disabled=True, width="large"),
            "notes": st.column_config.TextColumn("Notas", width="medium"),
        },
        use_container_width=True,
    )
    updated_steps = update_steps_from_editor(steps, edited_frame)

    current_ids = [step.id for step in sort_steps(updated_steps)]
    if not selected_step_id or selected_step_id not in current_ids:
        selected_step_id = current_ids[0]

    step_labels = {
        step.id: f"{step.step_order:02d} · {step.formula or 'sin formula'} · {step.df_name or 'sin df'}"
        for step in sort_steps(updated_steps)
    }
    selected_step_id = st.selectbox(
        "Paso seleccionado",
        options=current_ids,
        format_func=lambda step_id: step_labels[step_id],
        index=current_ids.index(selected_step_id),
        key=f"{key_prefix}-selected-step",
    )

    controls = st.columns(5)
    if controls[0].button("Anadir paso", key=f"{key_prefix}-add"):
        next_order = max(step.step_order for step in updated_steps) + 1
        updated_steps.append(
            new_pipeline_step(
                step_order=next_order,
                df_name=active_df_name or (df_names[0] if df_names else ""),
                formula=formula_names[0] if formula_names else "",
            )
        )
        updated_steps = sort_steps(updated_steps)
        selected_step_id = updated_steps[-1].id

    if controls[1].button("Duplicar", key=f"{key_prefix}-duplicate") and selected_step_id:
        selected_step = next(step for step in updated_steps if step.id == selected_step_id)
        duplicate = clone_pipeline_step(selected_step, step_order=selected_step.step_order + 1)
        updated_steps.append(duplicate)
        updated_steps = _reindex_steps(updated_steps)
        selected_step_id = duplicate.id

    if controls[2].button("Eliminar", key=f"{key_prefix}-delete") and selected_step_id:
        updated_steps = [step for step in updated_steps if step.id != selected_step_id]
        updated_steps = _reindex_steps(updated_steps)
        selected_step_id = updated_steps[0].id if updated_steps else None

    if controls[3].button("Subir", key=f"{key_prefix}-move-up") and selected_step_id:
        updated_steps = _move_step(updated_steps, selected_step_id, direction=-1)

    if controls[4].button("Bajar", key=f"{key_prefix}-move-down") and selected_step_id:
        updated_steps = _move_step(updated_steps, selected_step_id, direction=1)

    return sort_steps(updated_steps), selected_step_id


def _move_step(steps: list[PipelineStep], step_id: str, *, direction: int) -> list[PipelineStep]:
    ordered = sort_steps([deepcopy(step) for step in steps])
    index = next(index for index, step in enumerate(ordered) if step.id == step_id)
    new_index = max(0, min(len(ordered) - 1, index + direction))
    if new_index == index:
        return ordered
    ordered[index], ordered[new_index] = ordered[new_index], ordered[index]
    return _reindex_steps(ordered)


def _reindex_steps(steps: list[PipelineStep]) -> list[PipelineStep]:
    ordered = sort_steps(steps)
    for index, step in enumerate(ordered, start=1):
        step.step_order = index
    return ordered
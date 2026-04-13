from __future__ import annotations

from dataclasses import asdict

import pandas as pd
import streamlit as st

from caitsith_studio.models import DataFrameRegistry, PipelineResult, PreviewArtifact


def render_result_area(
    *,
    dataframe_registry: DataFrameRegistry,
    selected_df_name: str | None,
    preview_artifact: PreviewArtifact | None,
    result: PipelineResult | None,
) -> None:
    st.subheader("Vista previa y trazabilidad")
    tabs = st.tabs(["Preview", "DataFrame", "Logs", "Codigo"])

    with tabs[0]:
        if preview_artifact is None:
            st.info("Genera una previsualizacion o ejecuta el pipeline para ver el diff del paso seleccionado.")
        else:
            _render_preview_artifact(preview_artifact)

    with tabs[1]:
        current_df = _current_frame(dataframe_registry, result, selected_df_name)
        if current_df is None:
            st.info("No hay un DataFrame seleccionado para mostrar.")
        else:
            st.caption(f"Shape actual: {current_df.shape[0]} filas x {current_df.shape[1]} columnas")
            st.dataframe(current_df.head(50), use_container_width=True)

    with tabs[2]:
        if result is None or not result.logs:
            st.info("Todavia no hay logs de ejecucion.")
        else:
            log_frame = pd.DataFrame([asdict(log) for log in result.logs])
            st.dataframe(log_frame, use_container_width=True)

    with tabs[3]:
        if result is None or not result.generated_code:
            st.info("Todavia no se ha generado el codigo Python equivalente del pipeline.")
        else:
            st.code(result.generated_code, language="python")


def _render_preview_artifact(preview_artifact: PreviewArtifact) -> None:
    st.write(preview_artifact.summary)

    if preview_artifact.validation_errors:
        for error in preview_artifact.validation_errors:
            st.error(error)

    if preview_artifact.validation_warnings:
        for warning in preview_artifact.validation_warnings:
            st.warning(warning)

    metrics = st.columns(4)
    metrics[0].metric("Filas delta", preview_artifact.row_delta)
    metrics[1].metric("Nuevas columnas", len(preview_artifact.new_columns))
    metrics[2].metric("Columnas cambiadas", len(preview_artifact.changed_columns))
    metrics[3].metric("Columnas eliminadas", len(preview_artifact.removed_columns))

    if preview_artifact.new_columns:
        st.caption(f"Columnas nuevas: {', '.join(preview_artifact.new_columns)}")
    if preview_artifact.changed_columns:
        st.caption(f"Columnas modificadas: {', '.join(preview_artifact.changed_columns[:12])}")
    if preview_artifact.removed_columns:
        st.caption(f"Columnas eliminadas: {', '.join(preview_artifact.removed_columns)}")

    before_col, after_col = st.columns(2)
    with before_col:
        st.markdown("**Antes**")
        st.dataframe(preview_artifact.before_head, use_container_width=True)
    with after_col:
        st.markdown("**Despues**")
        st.dataframe(preview_artifact.after_head, use_container_width=True)


def _current_frame(
    dataframe_registry: DataFrameRegistry,
    result: PipelineResult | None,
    selected_df_name: str | None,
) -> pd.DataFrame | None:
    if selected_df_name is None:
        return None
    if result is not None and selected_df_name in result.frames:
        return result.frames[selected_df_name]
    if selected_df_name in dataframe_registry.frames:
        return dataframe_registry.frames[selected_df_name]
    return None
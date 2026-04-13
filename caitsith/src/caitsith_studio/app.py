from __future__ import annotations

from copy import deepcopy
import io
from pathlib import Path

import pandas as pd
import streamlit as st

from caitsith.core import __file__ as repo_caitsith_path
from caitsith_studio.core import (
    OperationRegistry,
    PipelineExecutor,
    generate_python_code,
    load_caitsith_from_bytes,
    load_caitsith_from_path,
    new_pipeline_step,
    pipeline_from_json,
    pipeline_from_yaml,
    pipeline_to_json,
    pipeline_to_yaml,
    sort_steps,
)
from caitsith_studio.examples.demo_caitsith import __file__ as demo_caitsith_path
from caitsith_studio.examples.sample_data import build_sample_frames
from caitsith_studio.models import DataFrameRegistry, PipelineResult, PipelineStep
from caitsith_studio.ui import render_pipeline_editor, render_result_area, render_step_form


DEFAULT_CAITSITH_PATH = Path(repo_caitsith_path).resolve()
DEMO_CAITSITH_PATH = Path(demo_caitsith_path).resolve()


def main() -> None:
    st.set_page_config(page_title="CaitSith Studio", layout="wide")
    _ensure_state()
    _ensure_default_source_loaded()

    st.title("CaitSith Studio")
    st.caption(
        "Constructor interactivo de transformaciones sobre pandas con introspeccion dinamica de tu clase CaitSith."
    )

    _render_sidebar()

    loaded_source = st.session_state.get("caitsith_source")
    operation_registry = st.session_state.get("operation_registry")
    dataframe_registry: DataFrameRegistry = st.session_state["dataframe_registry"]
    steps: list[PipelineStep] = st.session_state["pipeline_steps"]
    selected_step_id: str | None = st.session_state.get("selected_step_id")
    last_result: PipelineResult | None = st.session_state.get("last_result")
    last_preview = st.session_state.get("last_preview")

    if loaded_source is None or operation_registry is None:
        st.info(
            "No hay una fuente CaitSith cargada. Usa la barra lateral para cargar el core del repo, una demo, o un archivo `.py`/`.ipynb`."
        )
        return

    if not dataframe_registry.names():
        st.info("Carga o genera primero uno o varios DataFrames desde la barra lateral.")
        return

    if not steps:
        st.session_state["pipeline_steps"] = [
            new_pipeline_step(
                step_order=1,
                df_name=dataframe_registry.active_name or dataframe_registry.names()[0],
                formula=operation_registry.names()[0],
            )
        ]
        steps = st.session_state["pipeline_steps"]

    overview_col, details_col = st.columns([1.1, 1.4], gap="large")
    with overview_col:
        st.markdown("**DataFrames cargados**")
        st.dataframe(dataframe_registry.info_frame(), use_container_width=True)

        updated_steps, selected_step_id = render_pipeline_editor(
            steps=steps,
            df_names=dataframe_registry.names(),
            formula_names=operation_registry.names(),
            active_df_name=dataframe_registry.active_name,
            selected_step_id=selected_step_id,
        )
        st.session_state["pipeline_steps"] = updated_steps
        st.session_state["selected_step_id"] = selected_step_id
        steps = updated_steps

    with details_col:
        selected_step = next((step for step in steps if step.id == selected_step_id), None)
        if selected_step is None:
            st.warning("No hay un paso seleccionado actualmente.")
        else:
            form_result = render_step_form(
                step=selected_step,
                registry=operation_registry,
                dataframe_registry=dataframe_registry,
            )
            if form_result is not None:
                updated_step, save_clicked = form_result
                step_changed = any(
                    [
                        selected_step.df_name != updated_step.df_name,
                        selected_step.formula != updated_step.formula,
                        selected_step.enabled != updated_step.enabled,
                        selected_step.notes != updated_step.notes,
                        selected_step.parameters != updated_step.parameters,
                    ]
                )
                st.session_state["pipeline_steps"] = [
                    updated_step if step.id == updated_step.id else step
                    for step in steps
                ]
                steps = st.session_state["pipeline_steps"]

                current_preview = st.session_state.get("last_preview")
                if step_changed and current_preview is not None and current_preview.step_id == updated_step.id:
                    st.session_state["last_preview"] = None

                if save_clicked:
                    st.rerun()

            _render_execution_controls(operation_registry, dataframe_registry)

    render_result_area(
        dataframe_registry=dataframe_registry,
        selected_df_name=dataframe_registry.active_name,
        preview_artifact=last_preview,
        result=last_result,
    )


def _render_sidebar() -> None:
    st.sidebar.header("Configuracion")
    _render_source_loader()
    _render_dataframe_loader()
    _render_pipeline_io()
    _render_result_export()


def _render_source_loader() -> None:
    st.sidebar.subheader("1. Fuente de CaitSith")
    source_mode = st.sidebar.radio(
        "Origen del core",
        options=["CaitSith del repo", "Demo incluida", "Ruta local", "Subir archivo"],
        key="sidebar-source-mode",
    )

    try:
        if source_mode == "CaitSith del repo":
            if st.sidebar.button("Cargar CaitSith del repo", use_container_width=True):
                _load_caitsith_path(DEFAULT_CAITSITH_PATH)

        elif source_mode == "Demo incluida":
            if st.sidebar.button("Cargar demo", use_container_width=True):
                _load_caitsith_path(DEMO_CAITSITH_PATH)

        elif source_mode == "Ruta local":
            path_value = st.sidebar.text_input(
                "Ruta al `.py` o `.ipynb`",
                value="",
                placeholder=r"C:\ruta\caitsith_core.ipynb",
            )
            if st.sidebar.button("Cargar desde ruta", use_container_width=True):
                _load_caitsith_path(path_value)

        else:
            uploaded = st.sidebar.file_uploader(
                "Sube el core de CaitSith",
                type=["py", "ipynb"],
                key="sidebar-caitsith-upload",
            )
            if uploaded is not None and st.sidebar.button("Cargar archivo", use_container_width=True):
                loaded = load_caitsith_from_bytes(uploaded.name, uploaded.getvalue())
                st.session_state["caitsith_source"] = loaded
                st.session_state["operation_registry"] = OperationRegistry.from_caitsith_class(loaded.caitsith_class)
                st.sidebar.success(f"CaitSith cargado desde {uploaded.name}")
    except Exception as error:
        st.sidebar.error(str(error))

    loaded = st.session_state.get("caitsith_source")
    registry = st.session_state.get("operation_registry")
    if loaded is not None and registry is not None:
        st.sidebar.caption(f"Fuente activa: {loaded.source_name}")
        st.sidebar.caption(f"Operaciones detectadas: {len(registry.names())}")


def _render_dataframe_loader() -> None:
    st.sidebar.subheader("2. DataFrames")
    if st.sidebar.button("Cargar ejemplo minimo", use_container_width=True):
        registry: DataFrameRegistry = st.session_state["dataframe_registry"]
        for name, frame in build_sample_frames().items():
            registry.add(name, frame, source="sample", description="Dataset de ejemplo incluido")
        if registry.active_name is None and registry.names():
            registry.set_active(registry.names()[0])

    uploaded_files = st.sidebar.file_uploader(
        "Sube CSV o Excel",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="sidebar-dataframe-upload",
    )
    if uploaded_files and st.sidebar.button("Registrar DataFrames", use_container_width=True):
        registry = st.session_state["dataframe_registry"]
        for uploaded in uploaded_files:
            _register_uploaded_file(registry, uploaded.name, uploaded.getvalue())
        if registry.active_name is None and registry.names():
            registry.set_active(registry.names()[0])

    registry = st.session_state["dataframe_registry"]
    if registry.names():
        active_name = st.sidebar.selectbox(
            "DataFrame activo",
            options=registry.names(),
            index=registry.names().index(registry.active_name or registry.names()[0]),
        )
        registry.set_active(active_name)


def _render_pipeline_io() -> None:
    st.sidebar.subheader("3. Pipeline")
    steps: list[PipelineStep] = st.session_state["pipeline_steps"]

    if steps:
        st.sidebar.download_button(
            "Descargar pipeline JSON",
            data=pipeline_to_json(steps),
            file_name="caitsith_pipeline.json",
            mime="application/json",
            use_container_width=True,
        )
        st.sidebar.download_button(
            "Descargar pipeline YAML",
            data=pipeline_to_yaml(steps),
            file_name="caitsith_pipeline.yaml",
            mime="text/yaml",
            use_container_width=True,
        )

    pipeline_upload = st.sidebar.file_uploader(
        "Importar pipeline JSON/YAML",
        type=["json", "yaml", "yml"],
        key="sidebar-pipeline-upload",
    )
    if pipeline_upload is not None and st.sidebar.button("Cargar pipeline", use_container_width=True):
        raw_text = pipeline_upload.getvalue().decode("utf-8")
        if pipeline_upload.name.endswith(".json"):
            st.session_state["pipeline_steps"] = pipeline_from_json(raw_text)
        else:
            st.session_state["pipeline_steps"] = pipeline_from_yaml(raw_text)
        st.session_state["selected_step_id"] = st.session_state["pipeline_steps"][0].id if st.session_state["pipeline_steps"] else None

    if st.sidebar.button("Vaciar pipeline", use_container_width=True):
        st.session_state["pipeline_steps"] = []
        st.session_state["selected_step_id"] = None
        st.session_state["last_preview"] = None
        st.session_state["last_result"] = None


def _render_result_export() -> None:
    st.sidebar.subheader("4. Salidas")
    registry: DataFrameRegistry = st.session_state["dataframe_registry"]
    result: PipelineResult | None = st.session_state.get("last_result")
    active_name = registry.active_name
    if active_name is None:
        return

    current_df = None
    if result is not None and active_name in result.frames:
        current_df = result.frames[active_name]
    elif active_name in registry.frames:
        current_df = registry.frames[active_name]

    if current_df is not None:
        csv_bytes = current_df.to_csv(index=False).encode("utf-8")
        st.sidebar.download_button(
            "Exportar DataFrame actual a CSV",
            data=csv_bytes,
            file_name=f"{active_name}.csv",
            mime="text/csv",
            use_container_width=True,
        )


def _render_execution_controls(operation_registry: OperationRegistry, dataframe_registry: DataFrameRegistry) -> None:
    steps: list[PipelineStep] = st.session_state["pipeline_steps"]
    selected_step_id: str | None = st.session_state.get("selected_step_id")
    if selected_step_id is None:
        return

    executor = PipelineExecutor(operation_registry)
    controls = st.columns(5)
    preview_clicked = controls[0].button("Previsualizar paso", use_container_width=True)
    run_step_clicked = controls[1].button("Ejecutar paso", use_container_width=True)
    run_forward_clicked = controls[2].button("Ejecutar desde aqui", use_container_width=True)
    run_all_clicked = controls[3].button("Ejecutar pipeline", type="primary", use_container_width=True)
    rollback_clicked = controls[4].button("Rollback", use_container_width=True)

    if preview_clicked:
        try:
            base_frames, start_step_id = _resolve_execution_base(steps, selected_step_id)
            preview = executor.preview_step(
                dataframe_registry,
                steps,
                selected_step_id,
                base_frames=base_frames,
                start_at_step_id=start_step_id,
            )
            st.session_state["last_preview"] = preview
            st.session_state["pipeline_steps"] = _update_step_preview(steps, preview.step_id, preview.summary)
            st.rerun()
        except Exception as error:
            st.session_state["last_preview"] = None
            st.error(str(error))

    if run_step_clicked:
        _execute_pipeline(
            executor=executor,
            dataframe_registry=dataframe_registry,
            steps=steps,
            selected_step_id=selected_step_id,
            stop_after_step_id=selected_step_id,
        )
        st.rerun()

    if run_forward_clicked:
        _execute_pipeline(
            executor=executor,
            dataframe_registry=dataframe_registry,
            steps=steps,
            selected_step_id=selected_step_id,
            stop_after_step_id=None,
        )
        st.rerun()

    if run_all_clicked:
        _execute_pipeline(
            executor=executor,
            dataframe_registry=dataframe_registry,
            steps=steps,
            selected_step_id=None,
            stop_after_step_id=None,
        )
        st.rerun()

    if rollback_clicked:
        _rollback_to_previous_snapshot(dataframe_registry)
        st.rerun()


def _execute_pipeline(
    *,
    executor: PipelineExecutor,
    dataframe_registry: DataFrameRegistry,
    steps: list[PipelineStep],
    selected_step_id: str | None,
    stop_after_step_id: str | None,
) -> None:
    if selected_step_id is None:
        base_frames = None
        start_step_id = None
    else:
        base_frames, start_step_id = _resolve_execution_base(steps, selected_step_id)

    result = executor.execute(
        dataframe_registry,
        steps,
        base_frames=base_frames,
        start_at_step_id=start_step_id,
        stop_after_step_id=stop_after_step_id,
    )
    result.generated_code = generate_python_code(
        st.session_state["pipeline_steps"],
        caitsith_class=executor.registry.caitsith_class,
    )
    result = _merge_with_previous_result(result, start_step_id)
    st.session_state["last_result"] = result
    st.session_state["pipeline_steps"] = _apply_result_to_steps(st.session_state["pipeline_steps"], result)


def _resolve_execution_base(
    steps: list[PipelineStep],
    selected_step_id: str,
) -> tuple[dict[str, pd.DataFrame] | None, str | None]:
    previous_result: PipelineResult | None = st.session_state.get("last_result")
    if previous_result is None:
        return None, None

    ordered = sort_steps(steps)
    index = next((position for position, step in enumerate(ordered) if step.id == selected_step_id), None)
    if index is None or index == 0:
        return None, None

    previous_step_id = ordered[index - 1].id
    snapshot = previous_result.snapshots.get(previous_step_id)
    if snapshot is None:
        return None, None
    return deepcopy(snapshot), selected_step_id


def _merge_with_previous_result(current_result: PipelineResult, start_step_id: str | None) -> PipelineResult:
    if start_step_id is None:
        return current_result

    previous_result: PipelineResult | None = st.session_state.get("last_result")
    if previous_result is None:
        return current_result

    merged = PipelineResult(
        frames=current_result.frames,
        snapshots={**previous_result.snapshots, **current_result.snapshots},
        logs=current_result.logs,
        step_details={**previous_result.step_details, **current_result.step_details},
        generated_code=current_result.generated_code,
    )
    return merged


def _apply_result_to_steps(steps: list[PipelineStep], result: PipelineResult) -> list[PipelineStep]:
    updated_steps: list[PipelineStep] = []
    detail_status = {detail.step_id: detail for detail in result.step_details.values()}
    log_status = {log.step_id: log for log in result.logs}
    for step in steps:
        updated = deepcopy(step)
        detail = detail_status.get(step.id)
        log = log_status.get(step.id)
        if detail is not None:
            updated.status = detail.status
            updated.preview = detail.message
            updated.last_warning = detail.warning
            updated.last_error = None if detail.status != "error" else detail.message
        elif log is not None:
            updated.status = log.status
            updated.preview = log.message
            updated.last_warning = log.warning
            updated.last_error = log.message if log.status == "error" else None
        else:
            updated.status = "pending"
        updated_steps.append(updated)
    return updated_steps


def _update_step_preview(steps: list[PipelineStep], step_id: str, summary: str) -> list[PipelineStep]:
    updated_steps: list[PipelineStep] = []
    for step in steps:
        current = deepcopy(step)
        if current.id == step_id:
            current.preview = summary
        updated_steps.append(current)
    return updated_steps


def _rollback_to_previous_snapshot(dataframe_registry: DataFrameRegistry) -> None:
    result: PipelineResult | None = st.session_state.get("last_result")
    selected_step_id: str | None = st.session_state.get("selected_step_id")
    steps: list[PipelineStep] = st.session_state["pipeline_steps"]
    if result is None or selected_step_id is None:
        return

    ordered = sort_steps(steps)
    index = next((position for position, step in enumerate(ordered) if step.id == selected_step_id), None)
    if index is None or index == 0:
        restored_frames = dataframe_registry.copy_frames()
        restored_snapshots: dict[str, dict[str, pd.DataFrame]] = {}
    else:
        previous_step_id = ordered[index - 1].id
        restored_frames = deepcopy(result.snapshots.get(previous_step_id, dataframe_registry.copy_frames()))
        restored_snapshots = {
            step_id: snapshot
            for step_id, snapshot in result.snapshots.items()
            if step_id == previous_step_id or any(step.id == step_id for step in ordered[:index])
        }

    st.session_state["last_result"] = PipelineResult(
        frames=restored_frames,
        snapshots=restored_snapshots,
        logs=result.logs,
        step_details=result.step_details,
        generated_code=result.generated_code,
    )


def _load_caitsith_path(path_value: str | Path) -> None:
    loaded = load_caitsith_from_path(path_value)
    st.session_state["caitsith_source"] = loaded
    st.session_state["operation_registry"] = OperationRegistry.from_caitsith_class(loaded.caitsith_class)


def _register_uploaded_file(registry: DataFrameRegistry, filename: str, content: bytes) -> None:
    suffix = Path(filename).suffix.lower()
    buffer = io.BytesIO(content)
    stem = Path(filename).stem
    if suffix == ".csv":
        frame = pd.read_csv(buffer)
        registry.add(stem, frame, source="upload", description=filename)
        return
    if suffix in {".xlsx", ".xls"}:
        workbook = pd.read_excel(buffer, sheet_name=None)
        for sheet_name, frame in workbook.items():
            registry.add(f"{stem}_{sheet_name}", frame, source="upload", description=filename)
        return
    raise ValueError(f"Formato de archivo no soportado: {filename}")


def _ensure_default_source_loaded() -> None:
    if st.session_state.get("caitsith_source") is not None and st.session_state.get("operation_registry") is not None:
        return

    try:
        _load_caitsith_path(DEFAULT_CAITSITH_PATH)
    except Exception:
        try:
            _load_caitsith_path(DEMO_CAITSITH_PATH)
        except Exception:
            pass


def _ensure_state() -> None:
    st.session_state.setdefault("caitsith_source", None)
    st.session_state.setdefault("operation_registry", None)
    st.session_state.setdefault("dataframe_registry", DataFrameRegistry())
    st.session_state.setdefault("pipeline_steps", [])
    st.session_state.setdefault("selected_step_id", None)
    st.session_state.setdefault("last_preview", None)
    st.session_state.setdefault("last_result", None)


if __name__ == "__main__":
    main()
from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd
import streamlit as st

from caitsith_studio.core.operation_registry import OperationRegistry
from caitsith_studio.models import DataFrameRegistry, OperationSpec, PipelineStep


OPERATORS = ["==", "!=", ">", ">=", "<", "<="]


def render_step_form(
    *,
    step: PipelineStep,
    registry: OperationRegistry,
    dataframe_registry: DataFrameRegistry,
    key_prefix: str = "step-editor",
) -> tuple[PipelineStep, bool] | None:
    st.subheader("Edicion del paso")
    if not registry.names():
        st.info("Carga primero una fuente valida de CaitSith para editar pasos.")
        return None

    df_names = dataframe_registry.names()
    if not df_names:
        st.info("Carga primero uno o mas DataFrames para configurar el paso.")
        return None

    default_formula = step.formula if step.formula in registry.names() else registry.names()[0]
    default_df_name = step.df_name if step.df_name in df_names else dataframe_registry.active_name or df_names[0]

    df_name = st.selectbox(
        "DataFrame objetivo",
        options=df_names,
        index=df_names.index(default_df_name),
        key=f"{key_prefix}-{step.id}-df-name",
    )
    formula = st.selectbox(
        "Formula / metodo",
        options=registry.names(),
        index=registry.names().index(default_formula),
        format_func=lambda name: registry.get(name).display_name,
        key=f"{key_prefix}-{step.id}-formula",
    )
    enabled = st.checkbox(
        "Paso habilitado",
        value=step.enabled,
        key=f"{key_prefix}-{step.id}-enabled",
    )
    notes = st.text_area(
        "Notas",
        value=step.notes,
        height=90,
        key=f"{key_prefix}-{step.id}-notes",
    )

    spec = registry.get(formula)
    st.caption(spec.signature_text)
    st.markdown(spec.docstring)
    st.caption(
        f"Categoria: {spec.category} | Tipo: {spec.kind} | Retorno: {spec.return_kind} | "
        f"Muta DataFrame: {'si' if spec.mutates_dataframe else 'no'}"
    )

    working_columns = dataframe_registry.get(df_name).columns.tolist()
    parameters = render_parameter_fields(
        spec=spec,
        existing_parameters=step.parameters,
        dataframe_registry=dataframe_registry,
        df_name=df_name,
        columns=working_columns,
        key_prefix=f"{key_prefix}-{step.id}-params",
    )

    updated = deepcopy(step)
    updated.df_name = df_name
    updated.formula = formula
    updated.enabled = enabled
    updated.notes = notes
    updated.parameters = parameters

    if _step_configuration_changed(step, updated):
        updated.status = "pending"
        updated.preview = ""
        updated.last_error = None
        updated.last_warning = None

    save_clicked = st.button("Guardar paso", type="primary", key=f"{key_prefix}-{step.id}-save")
    return updated, save_clicked


def _step_configuration_changed(original: PipelineStep, updated: PipelineStep) -> bool:
    return any(
        [
            original.df_name != updated.df_name,
            original.formula != updated.formula,
            original.enabled != updated.enabled,
            original.notes != updated.notes,
            original.parameters != updated.parameters,
        ]
    )


def render_parameter_fields(
    *,
    spec: OperationSpec,
    existing_parameters: dict[str, Any],
    dataframe_registry: DataFrameRegistry,
    df_name: str,
    columns: list[str],
    key_prefix: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    parameter_map = {parameter.name: parameter for parameter in spec.parameters}
    handled: set[str] = set()
    group_count = 0

    for parameter in spec.parameters:
        if parameter.name in handled:
            continue

        if parameter.name == "condition_columns" and {"condition_values"}.issubset(parameter_map):
            selected_columns, selected_values = _render_condition_pairs_editor(
                label=parameter.label,
                columns=columns,
                existing_columns=existing_parameters.get("condition_columns", []),
                existing_values=existing_parameters.get("condition_values", []),
                key=f"{key_prefix}-condition-pairs",
            )
            params["condition_columns"] = selected_columns
            params["condition_values"] = selected_values
            handled.update({"condition_columns", "condition_values"})
            continue

        if parameter.name == "condition_columns" and {"lower_bounds", "upper_bounds"}.issubset(parameter_map):
            selected_columns, lower_bounds, upper_bounds = _render_range_pairs_editor(
                columns=columns,
                existing_columns=existing_parameters.get("condition_columns", []),
                existing_lower=existing_parameters.get("lower_bounds", []),
                existing_upper=existing_parameters.get("upper_bounds", []),
                key=f"{key_prefix}-range-pairs",
            )
            params["condition_columns"] = selected_columns
            params["lower_bounds"] = lower_bounds
            params["upper_bounds"] = upper_bounds
            handled.update({"condition_columns", "lower_bounds", "upper_bounds"})
            continue

        if parameter.name == "conditions":
            params[parameter.name] = _render_conditions_editor(
                columns=columns,
                existing_value=existing_parameters.get(parameter.name, []),
                key=f"{key_prefix}-conditions",
            )
            handled.add(parameter.name)
            continue

        if parameter.name == "condition_groups":
            grouped_conditions = _render_condition_groups_editor(
                columns=columns,
                existing_value=existing_parameters.get(parameter.name, []),
                key=f"{key_prefix}-condition-groups",
            )
            params[parameter.name] = grouped_conditions
            group_count = len(grouped_conditions)
            handled.add(parameter.name)
            continue

        if parameter.name == "result_values" and "condition_groups" in parameter_map:
            params[parameter.name] = _render_value_list_editor(
                label=parameter.label,
                existing_value=existing_parameters.get(parameter.name, []),
                key=f"{key_prefix}-result-values",
                expected_rows=group_count or None,
            )
            handled.add(parameter.name)
            continue

        params[parameter.name] = _render_single_parameter(
            parameter_name=parameter.name,
            parameter_label=parameter.label,
            widget=parameter.widget,
            choices=parameter.choices,
            default=parameter.default,
            existing_value=existing_parameters.get(parameter.name),
            dataframe_registry=dataframe_registry,
            columns=columns,
            key=f"{key_prefix}-{parameter.name}",
        )
        handled.add(parameter.name)

    return params


def _render_single_parameter(
    *,
    parameter_name: str,
    parameter_label: str,
    widget: str,
    choices: list[Any],
    default: Any,
    existing_value: Any,
    dataframe_registry: DataFrameRegistry,
    columns: list[str],
    key: str,
) -> Any:
    value = default if existing_value is None else existing_value

    if widget == "column":
        options = columns or [""]
        safe_value = value if value in options else (options[0] if options else "")
        return st.selectbox(parameter_label, options=options, index=options.index(safe_value), key=key)

    if widget == "columns":
        selected = [column for column in (value or []) if column in columns]
        return st.multiselect(parameter_label, options=columns, default=selected, key=key)

    if widget == "dataframe":
        options = [""] + dataframe_registry.names()
        safe_value = value if value in options else ""
        return st.selectbox(parameter_label, options=options, index=options.index(safe_value), key=key) or None

    if widget == "literal":
        if not choices:
            return st.text_input(parameter_label, value="" if value is None else str(value), key=key)
        safe_value = value if value in choices else choices[0]
        return st.selectbox(parameter_label, options=choices, index=choices.index(safe_value), key=key)

    if widget == "bool":
        return st.checkbox(parameter_label, value=bool(value), key=key)

    if widget == "number":
        return _render_number_input(parameter_label, value, key)

    if widget == "value_list":
        return _render_value_list_editor(label=parameter_label, existing_value=value or [], key=key)

    return st.text_input(parameter_label, value="" if value is None else str(value), key=key)


def _render_number_input(label: str, value: Any, key: str) -> Any:
    if isinstance(value, int) and not isinstance(value, bool):
        return st.number_input(label, value=int(value), step=1, key=key)
    if isinstance(value, float):
        return st.number_input(label, value=float(value), key=key)
    raw_value = st.text_input(label, value="" if value is None else str(value), key=key)
    return _coerce_scalar(raw_value)


def _render_conditions_editor(*, columns: list[str], existing_value: list[Any], key: str) -> list[tuple[str, str, Any]]:
    rows = []
    for condition in existing_value:
        if len(condition) >= 3:
            rows.append({"column": condition[0], "operator": condition[1], "value": condition[2]})
    if not rows:
        rows = [{"column": columns[0] if columns else "", "operator": "==", "value": ""}]

    frame = pd.DataFrame(rows)
    edited = st.data_editor(
        frame,
        num_rows="dynamic",
        hide_index=True,
        key=key,
        column_config={
            "column": st.column_config.SelectboxColumn("Columna", options=columns),
            "operator": st.column_config.SelectboxColumn("Operador", options=OPERATORS),
            "value": st.column_config.TextColumn("Valor"),
        },
    )
    output: list[tuple[str, str, Any]] = []
    for row in edited.to_dict(orient="records"):
        column_name = str(row.get("column") or "").strip()
        operator = str(row.get("operator") or "==").strip()
        if not column_name:
            continue
        output.append((column_name, operator, _coerce_scalar(row.get("value"))))
    return output


def _render_condition_groups_editor(*, columns: list[str], existing_value: list[Any], key: str) -> list[list[tuple[str, str, Any]]]:
    rows = []
    for group_index, group in enumerate(existing_value, start=1):
        for condition in group:
            if len(condition) >= 3:
                rows.append(
                    {
                        "group": group_index,
                        "column": condition[0],
                        "operator": condition[1],
                        "value": condition[2],
                    }
                )
    if not rows:
        rows = [{"group": 1, "column": columns[0] if columns else "", "operator": "==", "value": ""}]

    frame = pd.DataFrame(rows)
    edited = st.data_editor(
        frame,
        num_rows="dynamic",
        hide_index=True,
        key=key,
        column_config={
            "group": st.column_config.NumberColumn("Grupo", min_value=1, step=1),
            "column": st.column_config.SelectboxColumn("Columna", options=columns),
            "operator": st.column_config.SelectboxColumn("Operador", options=OPERATORS),
            "value": st.column_config.TextColumn("Valor"),
        },
    )

    grouped: dict[int, list[tuple[str, str, Any]]] = {}
    for row in edited.to_dict(orient="records"):
        column_name = str(row.get("column") or "").strip()
        if not column_name:
            continue
        group_id = int(row.get("group") or 1)
        grouped.setdefault(group_id, []).append(
            (column_name, str(row.get("operator") or "=="), _coerce_scalar(row.get("value")))
        )
    return [grouped[group_id] for group_id in sorted(grouped)]


def _render_condition_pairs_editor(
    *,
    label: str,
    columns: list[str],
    existing_columns: list[Any],
    existing_values: list[Any],
    key: str,
) -> tuple[list[str], list[Any]]:
    rows = []
    max_len = max(len(existing_columns), len(existing_values), 1)
    for index in range(max_len):
        rows.append(
            {
                "column": existing_columns[index] if index < len(existing_columns) else (columns[0] if columns else ""),
                "value": existing_values[index] if index < len(existing_values) else "",
            }
        )

    edited = st.data_editor(
        pd.DataFrame(rows),
        num_rows="dynamic",
        hide_index=True,
        key=key,
        column_config={
            "column": st.column_config.SelectboxColumn(label, options=columns),
            "value": st.column_config.TextColumn("Valor"),
        },
    )
    output_columns: list[str] = []
    output_values: list[Any] = []
    for row in edited.to_dict(orient="records"):
        column_name = str(row.get("column") or "").strip()
        if not column_name:
            continue
        output_columns.append(column_name)
        output_values.append(_coerce_scalar(row.get("value")))
    return output_columns, output_values


def _render_range_pairs_editor(
    *,
    columns: list[str],
    existing_columns: list[Any],
    existing_lower: list[Any],
    existing_upper: list[Any],
    key: str,
) -> tuple[list[str], list[Any], list[Any]]:
    rows = []
    max_len = max(len(existing_columns), len(existing_lower), len(existing_upper), 1)
    for index in range(max_len):
        rows.append(
            {
                "column": existing_columns[index] if index < len(existing_columns) else (columns[0] if columns else ""),
                "lower": existing_lower[index] if index < len(existing_lower) else "",
                "upper": existing_upper[index] if index < len(existing_upper) else "",
            }
        )

    edited = st.data_editor(
        pd.DataFrame(rows),
        num_rows="dynamic",
        hide_index=True,
        key=key,
        column_config={
            "column": st.column_config.SelectboxColumn("Columna", options=columns),
            "lower": st.column_config.TextColumn("Limite inferior"),
            "upper": st.column_config.TextColumn("Limite superior"),
        },
    )
    output_columns: list[str] = []
    output_lower: list[Any] = []
    output_upper: list[Any] = []
    for row in edited.to_dict(orient="records"):
        column_name = str(row.get("column") or "").strip()
        if not column_name:
            continue
        output_columns.append(column_name)
        output_lower.append(_coerce_scalar(row.get("lower")))
        output_upper.append(_coerce_scalar(row.get("upper")))
    return output_columns, output_lower, output_upper


def _render_value_list_editor(*, label: str, existing_value: list[Any], key: str, expected_rows: int | None = None) -> list[Any]:
    rows = [{"value": value} for value in existing_value]
    if not rows:
        row_count = expected_rows or 1
        rows = [{"value": ""} for _ in range(row_count)]

    edited = st.data_editor(
        pd.DataFrame(rows),
        num_rows="dynamic",
        hide_index=True,
        key=key,
        column_config={
            "value": st.column_config.TextColumn(label),
        },
    )
    output: list[Any] = []
    for row in edited.to_dict(orient="records"):
        if row.get("value") in {None, ""}:
            continue
        output.append(_coerce_scalar(row.get("value")))
    return output


def _coerce_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value).strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"none", "null", "nan", "na"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if "." not in text and "e" not in lowered:
            return int(text)
        return float(text)
    except ValueError:
        return text
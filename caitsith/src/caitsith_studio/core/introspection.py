from __future__ import annotations

import inspect
from typing import Any, Literal, get_args, get_origin

from caitsith_studio.models import OperationKind, OperationParameter, OperationReturnKind, OperationSpec


LOOKUP_NAMES = {
    "buscarv",
    "buscarh",
    "buscarx",
    "buscarx_multiple",
    "buscarv_multiple",
    "buscarh_multiple",
    "indice_coincidir",
    "coincidir",
    "coincidir_posicion",
    "buscarv_multiple_conditions",
    "buscarh_multiple_conditions",
}
TEXT_NAMES = {
    "extrae",
    "extraer",
    "izquierda",
    "derecha",
    "dividir_texto",
    "reemplazar",
    "reemplazar_multiple",
    "reemplazar_posicion",
    "texto_unir",
    "concatenar",
    "concatenar_secuencia",
    "espacios",
    "limpiar_texto",
    "texto_antes",
    "texto_despues",
    "texto_entre",
    "contar_ocurrencias",
    "extraer_regex",
    "invertir_texto",
    "nompropio",
    "mayusc",
    "minusc",
    "sustituir",
    "sustiuir",
    "eliminar_caracteres",
    "rellenar_izquierda",
    "rellenar_derecha",
}
DATE_NAMES = {
    "fecha_a_texto",
    "texto_a_fecha",
    "diferencia_fechas",
    "dias",
    "horas",
    "marcar_como_componente_fecha",
    "construir_fecha_desde_partes",
    "dia_del_mes",
    "hora_del_dia",
    "minuto",
    "segundo",
    "semana_del_ano",
    "trimestre",
    "nombre_mes",
    "nombre_dia",
    "es_fin_de_semana",
    "periodo_anio_mes",
    "inicio_mes",
    "fin_mes",
    "inicio_trimestre",
    "fin_trimestre",
    "es_dia_habil",
    "dias_habiles_entre",
    "sumar_dias_habiles",
    "dias_laborables_entre",
    "dia_laborable",
    "hoy",
    "ahora",
    "diasem",
    "mes",
    "ano",
    "inicio_ano",
    "fin_ano",
    "dia_del_ano",
    "dias_en_mes",
    "sumar_meses",
    "sumar_anos",
    "edad_anos",
}
FINANCE_NAMES = {"pago", "valor_futuro", "valor_presente", "periodos_pago", "pmt", "fv", "pv", "nper", "formatear_monedas"}
SORT_NAMES = {"ordenar", "ordenar_por"}
ROW_FILTER_NAMES = {"filtrar", "filtro_avanzado"}
GROUPBY_NAMES = {"agrupar_transformar", "ranking_por_grupo", "acumulado_por_grupo", "promedio_movil", "suma_movil"}
MATH_NAMES = {
    "sumar",
    "restar",
    "multiplicar",
    "dividir",
    "dividir_aritmetica",
    "promedio",
    "mediana",
    "producto",
    "maximo",
    "minimo",
    "potencia",
    "absoluto",
    "raiz",
    "logaritmo",
    "ln",
    "exp",
    "seno",
    "coseno",
    "tangente",
    "redondear",
    "redondear_mas",
    "redondear_menos",
    "redondear_basico",
    "redondear_multiplo",
    "truncar",
    "residuo",
    "signo",
}
CONDITIONAL_PREFIXES = ("si", "sumar_si", "promedio_si", "contar_si", "max_si", "min_si", "restar_si")


def build_operation_specs(caitsith_class: type) -> list[OperationSpec]:
    methods = inspect.getmembers(caitsith_class, predicate=inspect.isfunction)
    specs: list[OperationSpec] = []
    for name, method in methods:
        if name.startswith("_"):
            continue
        if name == "set_options":
            continue
        specs.append(build_operation_spec(name=name, method=method))
    return sorted(specs, key=lambda spec: (spec.category, spec.display_name))


def build_operation_spec(name: str, method: Any) -> OperationSpec:
    signature = inspect.signature(method)
    parameters = [
        build_operation_parameter(parameter)
        for parameter_name, parameter in signature.parameters.items()
        if parameter_name != "self"
    ]
    docstring = inspect.getdoc(method) or "Sin documentacion disponible."
    kind = infer_operation_kind(name)
    return_kind = infer_return_kind(name, method, docstring, parameters)
    creates_column = any(parameter.is_output_name for parameter in parameters)
    returns_dataframe = return_kind == "dataframe"
    mutates_dataframe = return_kind == "mutates_df" or (creates_column and return_kind != "dataframe")
    display_name = name.replace("_", " ").title()
    category = kind.replace("_", " ").title()
    signature_text = f"{name}{signature}"
    return OperationSpec(
        name=name,
        display_name=display_name,
        category=category,
        kind=kind,
        docstring=docstring,
        signature_text=signature_text,
        parameters=parameters,
        return_kind=return_kind,
        mutates_dataframe=mutates_dataframe,
        creates_column=creates_column,
        returns_dataframe=returns_dataframe,
    )


def build_operation_parameter(parameter: inspect.Parameter) -> OperationParameter:
    annotation_text = normalize_annotation(parameter.annotation)
    literal_choices = infer_literal_choices(parameter.annotation)
    widget = infer_widget_kind(parameter.name, parameter.annotation, parameter.default, literal_choices)
    return OperationParameter(
        name=parameter.name,
        label=parameter.name.replace("_", " ").capitalize(),
        annotation=annotation_text,
        required=parameter.default is inspect._empty,
        default=None if parameter.default is inspect._empty else parameter.default,
        widget=widget,
        choices=literal_choices,
        is_output_name=parameter.name in {"new_column_name", "new_column_prefix"} or parameter.name.endswith("_new_column_name"),
        accepts_column_values=widget in {"column", "columns", "conditions", "condition_groups", "condition_pairs", "range_pairs"},
        editor_columns=infer_editor_columns(parameter.name, widget),
    )


def infer_operation_kind(name: str) -> OperationKind:
    if name in LOOKUP_NAMES or "buscar" in name or "coincidir" in name:
        return "lookup"
    if name in ROW_FILTER_NAMES:
        return "row_filter"
    if name in TEXT_NAMES:
        return "text_transform"
    if name in DATE_NAMES:
        return "date_time"
    if name in FINANCE_NAMES:
        return "finance"
    if name in SORT_NAMES:
        return "sort"
    if name in GROUPBY_NAMES:
        return "groupby"
    if name in MATH_NAMES:
        return "math"
    if name.startswith(CONDITIONAL_PREFIXES) or name in {"exacto", "y_o", "elegir"}:
        return "conditional"
    if name.endswith("_agg") or name in {"moda", "var_p", "var_s", "desv_p", "desv_s", "percentil", "correlacion", "covarianza_p", "covarianza_s", "cuartil", "sumaproducto"}:
        return "aggregation"
    return "utility"


def infer_return_kind(
    name: str,
    method: Any,
    docstring: str,
    parameters: list[OperationParameter],
) -> OperationReturnKind:
    annotation = inspect.signature(method).return_annotation
    if annotation is not inspect._empty:
        normalized = normalize_annotation(annotation)
        if "DataFrame" in normalized:
            return "dataframe"
        if "Series" in normalized:
            return "series"
        if normalized in {"int", "float", "Union[int, float]", "Any"}:
            return "scalar"
        if normalized == "None":
            return "mutates_df"

    if name in {"filtrar", "ordenar", "unicos"}:
        return "dataframe"
    if name.endswith("_agg") or name in {"moda", "var_p", "var_s", "desv_p", "desv_s", "percentil", "correlacion", "covarianza_p", "covarianza_s", "cuartil", "sumaproducto"}:
        return "scalar"
    if name in {"contar_valores_agg", "contar_valores", "contar_valores_unicos_agg", "contar_valores_unicos"}:
        return "series"
    if any(parameter.is_output_name for parameter in parameters):
        return "mutates_df"
    if "Devuelve una copia" in docstring or "Devuelve un DataFrame" in docstring:
        return "dataframe"
    return "unknown"


def infer_widget_kind(name: str, annotation: Any, default: Any, literal_choices: list[Any]) -> str:
    if literal_choices:
        return "literal"
    if name == "external_df":
        return "dataframe"
    if name == "conditions":
        return "conditions"
    if name == "condition_groups":
        return "condition_groups"
    if name == "holidays":
        return "value_list"
    if name in {"condition_columns", "condition_values"}:
        return "condition_pairs"
    if name in {"lower_bounds", "upper_bounds"}:
        return "range_pairs"
    if name.endswith("_columns") or name in {"columns", "return_columns", "sum_columns", "average_columns", "subtract_columns", "group_columns"}:
        return "columns"
    if name.endswith("_column") or name == "column":
        return "column"

    origin = get_origin(annotation)
    if annotation is bool or isinstance(default, bool):
        return "bool"
    if annotation in {int, float}:
        return "number"
    if origin is Literal:
        return "literal"
    if origin in {list, tuple}:
        return "value_list"
    if isinstance(default, (int, float)) and not isinstance(default, bool):
        return "number"
    return "text"


def infer_literal_choices(annotation: Any) -> list[Any]:
    if get_origin(annotation) is Literal:
        return list(get_args(annotation))
    return []


def infer_editor_columns(name: str, widget: str) -> list[str]:
    if widget == "conditions":
        return ["column", "operator", "value"]
    if widget == "condition_groups":
        return ["group", "column", "operator", "value"]
    if widget == "condition_pairs":
        return ["column", "value"]
    if widget == "range_pairs":
        return ["column", "lower", "upper"]
    if widget == "value_list":
        return ["value"]
    return []


def normalize_annotation(annotation: Any) -> str:
    if annotation is inspect._empty:
        return "Any"
    if isinstance(annotation, str):
        return annotation
    origin = get_origin(annotation)
    if origin is None:
        return getattr(annotation, "__name__", str(annotation))
    args = ", ".join(normalize_annotation(arg) for arg in get_args(annotation))
    origin_name = getattr(origin, "__name__", str(origin))
    return f"{origin_name}[{args}]"
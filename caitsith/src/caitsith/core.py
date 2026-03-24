import numpy as np
import pandas as pd
from typing import Union, List, Tuple, Callable, Optional, Literal
import html
import re

class CaitSith:

    """
    Utilidades de transformación para DataFrames de pandas con una API inspirada en funciones de Excel.

    Incluye búsquedas, operaciones condicionales, texto, fechas, agregaciones y validaciones.
    """
    def __init__(self, df: pd.DataFrame, errors: Literal["raise", "coerce"] = "raise"):
        if isinstance(df, pd.DataFrame):
            self.df = df
        else:
            raise ValueError("El input debe ser un DataFrame de pandas")
        if errors not in {"raise", "coerce"}:
            raise ValueError("'errors' debe ser 'raise' o 'coerce'.")
        self.errors = errors

    def set_options(self, errors: Optional[Literal["raise", "coerce"]] = None) -> None:
        """
        Actualiza opciones globales de comportamiento de la instancia.
        """
        if errors is not None:
            if errors not in {"raise", "coerce"}:
                raise ValueError("'errors' debe ser 'raise' o 'coerce'.")
            self.errors = errors

    def _as_column_list(self, columns: Union[str, List[str]]) -> List[str]:
        if isinstance(columns, str):
            return [columns]
        if not isinstance(columns, list) or not columns:
            raise ValueError("Debes proporcionar una columna (str) o una lista no vacía de columnas.")
        if any(not isinstance(column, str) for column in columns):
            raise ValueError("Todos los elementos de 'columns' deben ser strings.")
        return columns

    def _validate_columns(self, columns: List[str]) -> None:
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

    def _validate_column(self, column: str) -> None:
        self._validate_columns([column])

    def _validate_same_length(self, *values: List) -> None:
        lengths = {len(value) for value in values}
        if len(lengths) > 1:
            raise ValueError("Las listas de entrada deben tener la misma longitud.")

    def _to_datetime_series(
        self,
        column: str,
        format_string: Optional[str] = None,
        dayfirst: bool = False,
        yearfirst: bool = False,
        errors: Optional[Literal["raise", "coerce"]] = None,
    ) -> pd.Series:
        self._validate_column(column)

        series = self.df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            return series

        mode = errors if errors is not None else self.errors
        parse_errors = "raise" if mode == "raise" else "coerce"

        return pd.to_datetime(
            series,
            format=format_string,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
            errors=parse_errors,
        )

    def _normalize_holidays(self, holidays: Optional[List[Union[str, pd.Timestamp]]]) -> Optional[np.ndarray]:
        if holidays is None:
            return None
        if not isinstance(holidays, list):
            raise ValueError("'holidays' debe ser una lista de fechas o None.")
        if not holidays:
            return None
        holiday_index = pd.to_datetime(holidays, errors="coerce")
        holiday_index = holiday_index[~holiday_index.isna()]
        if len(holiday_index) == 0:
            return None
        return holiday_index.normalize().to_numpy(dtype="datetime64[D]")

    def _handle_error(self, error: Exception, errors: Optional[Literal["raise", "coerce"]]) -> None:
        mode = errors if errors is not None else self.errors
        if mode == "raise":
            raise error

    def _resolve_conditional_value(self, value: Union[str, int, float, Callable, pd.Series]) -> pd.Series:
        if callable(value):
            return self.df.apply(value, axis=1)
        if isinstance(value, pd.Series):
            return value.reindex(self.df.index)
        if isinstance(value, str) and value in self.df.columns:
            return self.df[value]
        return pd.Series([value] * len(self.df), index=self.df.index)

    def _evaluate_condition(self, column: str, operator: str, value: Union[str, int, float]) -> pd.Series:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        series = self.df[column]
        if operator == "==":
            return series == value
        if operator == "!=":
            return series != value
        if operator == ">":
            return series > value
        if operator == "<":
            return series < value
        if operator == ">=":
            return series >= value
        if operator == "<=":
            return series <= value

        raise ValueError(f"Operador '{operator}' no soportado.")

    def _reduce_conditions(self, condition_results: List[pd.Series], logic: str) -> pd.Series:
        logic_lower = logic.lower()
        if logic_lower == "and":
            return np.logical_and.reduce(condition_results)
        if logic_lower == "or":
            return np.logical_or.reduce(condition_results)
        raise ValueError("El valor de 'logic' debe ser 'and' o 'or'.")

    def _infer_lookup_values_for_row(self, lookup_row: int) -> pd.Series:
        if lookup_row in self.df.columns:
            return self.df[lookup_row]
        if isinstance(lookup_row, int) and 0 <= lookup_row < self.df.shape[1]:
            return self.df.iloc[:, lookup_row]
        raise ValueError(
            "No se pudo inferir el valor a buscar por fila. "
            "Usa 'lookup_value_column' para indicar explícitamente los valores de búsqueda."
        )

    def buscarv(self, lookup_column: str, return_column: str, new_column_name: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Aplica la función BUSCARV a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        lookup_column: La columna en la que se buscará el valor.
        return_column: La columna de la cual se devolverá el valor.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        lookup_value_column: (opcional) Nombre de la columna que contiene los valores a buscar (si no se especifica, se usa la fila actual).
        not_found_value: El valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df
        
        if lookup_column not in df_to_use.columns or return_column not in df_to_use.columns:
            raise ValueError("Las columnas especificadas no existen en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")
        
        lookup_series = (
            df_to_use[[lookup_column, return_column]]
            .drop_duplicates(subset=lookup_column, keep="first")
            .set_index(lookup_column)[return_column]
        )
        lookup_dict = lookup_series.to_dict()
        
        if lookup_value_column is not None:
            self.df[new_column_name] = self.df[lookup_value_column].map(lambda x: lookup_dict.get(x, not_found_value))
        else:
            self.df[new_column_name] = self.df[lookup_column].map(lambda x: lookup_dict.get(x, not_found_value))
    
    def quitar_duplicados(self, subset: Optional[List[str]] = None, keep: str = "first", new_column_name: Optional[str] = None) -> None:
        """
        Elimina filas duplicadas del DataFrame basándose en las columnas especificadas y agrega una nueva columna que indica si la fila es un duplicado o no.
        
        Parameters:
        subset: Lista de columnas a considerar para identificar duplicados. Si es None, se consideran todas las columnas.
        keep: Especifica qué duplicado conservar. Puede ser 'first', 'last' o False (elimina todos los duplicados).
        new_column_name: Nombre de la nueva columna que indicará si la fila es un duplicado (opcional). Si no se proporciona, se actualizará el DataFrame original.
        """
        if subset is not None:
            for column in subset:
                if column not in self.df.columns:
                    raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        
        duplicated_series = self.df.duplicated(subset=subset, keep=keep)
        
        if new_column_name:
            self.df[new_column_name] = duplicated_series
            return

        self.df.drop_duplicates(subset=subset, keep=keep, inplace=True)

    def buscarh(self, lookup_row: int, return_row: int, new_column_name: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Aplica la función BUSCARH a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        Si hay múltiples coincidencias en `lookup_row`, devuelve la primera de izquierda a derecha.
        
        Parameters:
        lookup_row: La fila en la que se buscará el valor.
        return_row: La fila de la cual se devolverá el valor.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        lookup_value_column: (opcional) Nombre de la columna que contiene los valores a buscar (si no se especifica, se usa la fila actual).
        not_found_value: El valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df
        
        if lookup_row not in df_to_use.index or return_row not in df_to_use.index:
            raise ValueError("Las filas especificadas no existen en el DataFrame.")
        
        lookup_series = df_to_use.loc[lookup_row]

        def get_match_value(value):
            if pd.isna(value):
                matches = lookup_series[lookup_series.isna()]
            else:
                matches = lookup_series[lookup_series == value]
            if matches.empty:
                return not_found_value
            matched_column = matches.index[0]
            return df_to_use.at[return_row, matched_column]

        if lookup_value_column is not None:
            if lookup_value_column not in self.df.columns:
                raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
            self.df[new_column_name] = self.df[lookup_value_column].map(get_match_value)
            return

        lookup_values = self._infer_lookup_values_for_row(lookup_row)

        self.df[new_column_name] = lookup_values.map(get_match_value)

    def sumar_si(self, condition_column: str, condition_value: Union[str, int, float], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la función SUMAR.SI a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        condition_column: La columna que se usará para evaluar la condición.
        condition_value: El valor que debe cumplir la condición.
        sum_columns: Las columnas cuyos valores se sumarán si se cumple la condición.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if condition_column not in self.df.columns:
            raise ValueError("La columna de condición especificada no existe en el DataFrame.")

        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_column] == condition_value
        row_sum = self.df[sum_columns_list].sum(axis=1)
        self.df[new_column_name] = row_sum.where(condition_met, np.nan)

    def sumar_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la lógica de SUMAR.SI con condición por rango y agrega una nueva columna con los resultados.

        Parameters:
        condition_column: La columna usada para evaluar el rango.
        lower_bound: Límite inferior del rango (incluido).
        upper_bound: Límite superior del rango (incluido).
        sum_columns: Las columnas cuyos valores se sumarán si se cumple la condición.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_column(condition_column)
        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        row_sum = self.df[sum_columns_list].sum(axis=1)
        self.df[new_column_name] = row_sum.where(condition_met, np.nan)

    def promedio_si(self, condition_column: str, condition_value: Union[str, int, float], average_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la lógica de PROMEDIO.SI y agrega una nueva columna con los resultados.

        Parameters:
        condition_column: La columna que se usará para evaluar la condición.
        condition_value: El valor que debe cumplir la condición.
        average_columns: Las columnas cuyos valores se promediarán si se cumple la condición.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        self._validate_column(condition_column)
        average_columns_list = self._as_column_list(average_columns)
        self._validate_columns(average_columns_list)

        condition_met = self.df[condition_column] == condition_value
        row_avg = self.df[average_columns_list].mean(axis=1)
        self.df[new_column_name] = row_avg.where(condition_met, np.nan)

    def promedio_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], average_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la lógica de PROMEDIO.SI con condición por rango y agrega una nueva columna con los resultados.
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_column(condition_column)
        average_columns_list = self._as_column_list(average_columns)
        self._validate_columns(average_columns_list)

        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        row_avg = self.df[average_columns_list].mean(axis=1)
        self.df[new_column_name] = row_avg.where(condition_met, np.nan)

    def promedio_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], average_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la lógica de PROMEDIO.SI.CONJUNTO y agrega una nueva columna con los resultados.
        """
        self._validate_same_length(condition_columns, condition_values)
        self._validate_columns(condition_columns)

        average_columns_list = self._as_column_list(average_columns)
        self._validate_columns(average_columns_list)

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        row_avg = self.df[average_columns_list].mean(axis=1)
        self.df[new_column_name] = row_avg.where(condition_met, np.nan)

    def sumar_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la función SUMAR.SI.CONJUNTO a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        condition_columns: Las columnas que se usarán para evaluar las condiciones.
        condition_values: Los valores que deben cumplir las condiciones (en el mismo orden que condition_columns).
        sum_columns: Las columnas cuyos valores se sumarán si se cumplen las condiciones.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if len(condition_columns) != len(condition_values):
            raise ValueError("Las listas de columnas y valores de condición deben tener la misma longitud.")
        
        for column in condition_columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        
        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        row_sum = self.df[sum_columns_list].sum(axis=1)
        self.df[new_column_name] = row_sum.where(condition_met, np.nan)

    def filtro_avanzado(self, conditions: List[Tuple[str, str, Union[str, int, float]]], logic: str, new_column_name: str) -> None:
        """
        Aplica un filtro avanzado basado en múltiples condiciones y lógica (AND/OR) y agrega una nueva columna con los resultados.
        
        Parameters:
        conditions: Lista de tuplas que contienen (columna, operador, valor). Ejemplo: [("columna1", "==", 10), ("columna2", ">", 5)]
        logic: 'and' para aplicar lógica AND, 'or' para aplicar lógica OR.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")
        
        condition_results = [
            self._evaluate_condition(condition_column, operator, value)
            for condition_column, operator, value in conditions
        ]
        final_condition = self._reduce_conditions(condition_results, logic)
        
        self.df[new_column_name] = final_condition  


        
    def restar_si(self, condition_column: str, condition_value: Union[str, int, float], subtract_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la función RESTAR.SI (hipotética) a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        condition_column: La columna que se usará para evaluar la condición.
        condition_value: El valor que debe cumplir la condición.
        subtract_columns: Las columnas cuyos valores se restarán si se cumple la condición.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if condition_column not in self.df.columns:
            raise ValueError("La columna de condición especificada no existe en el DataFrame.")

        subtract_columns_list = self._as_column_list(subtract_columns)
        self._validate_columns(subtract_columns_list)

        condition_met = self.df[condition_column] == condition_value
        values = self.df[subtract_columns_list]
        row_sub = values.iloc[:, 0]
        for column in subtract_columns_list[1:]:
            row_sub = row_sub - self.df[column]

        self.df[new_column_name] = row_sub.where(condition_met, np.nan)
    def restar_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], subtract_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Aplica la función RESTAR.SI.CONJUNTO (hipotética) a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        condition_columns: Las columnas que se usarán para evaluar las condiciones.
        condition_values: Los valores que deben cumplir las condiciones (en el mismo orden que condition_columns).
        subtract_columns: Las columnas cuyos valores se restarán si se cumplen las condiciones.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if len(condition_columns) != len(condition_values):
            raise ValueError("Las listas de columnas y valores de condición deben tener la misma longitud.")
        
        for column in condition_columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        
        subtract_columns_list = self._as_column_list(subtract_columns)
        self._validate_columns(subtract_columns_list)

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        values = self.df[subtract_columns_list]
        row_sub = values.iloc[:, 0]
        for column in subtract_columns_list[1:]:
            row_sub = row_sub - self.df[column]

        self.df[new_column_name] = row_sub.where(condition_met, np.nan)

    def indice_coincidir(self, lookup_column: str, return_column: str, new_column_name: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Aplica la combinación de las funciones ÍNDICE + COINCIDIR a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        lookup_column: La columna en la que se buscará el valor.
        return_column: La columna de la cual se devolverá el valor correspondiente.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        lookup_value_column: (opcional) Nombre de la columna que contiene los valores a buscar (si no se especifica, se usa la fila actual).
        not_found_value: El valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df
        
        if lookup_column not in df_to_use.columns or return_column not in df_to_use.columns:
            raise ValueError("Las columnas especificadas no existen en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")
        
        def get_match(x):
            mask = df_to_use[lookup_column].eq(x)
            if not mask.any():
                return not_found_value
            match_idx = mask[mask].index[0]
            return df_to_use.at[match_idx, return_column]
        
        if lookup_value_column is not None:
            self.df[new_column_name] = self.df[lookup_value_column].apply(get_match)
        else:
            self.df[new_column_name] = self.df[lookup_column].apply(get_match)

    def coincidir(self, lookup_column: str, new_column_name: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Devuelve la primera coincidencia de índice para cada valor buscado, similar a COINCIDIR en Excel.

        Parameters:
        lookup_column: La columna donde se buscarán las coincidencias.
        new_column_name: El nombre de la nueva columna de salida.
        lookup_value_column: (opcional) Columna con los valores a buscar.
        not_found_value: Valor devuelto cuando no existe coincidencia.
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df

        if lookup_column not in df_to_use.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")

        lookup_df = df_to_use[[lookup_column]].copy()
        lookup_df["__coincidir_idx__"] = df_to_use.index
        lookup_series = (
            lookup_df
            .drop_duplicates(subset=lookup_column, keep="first")
            .set_index(lookup_column)["__coincidir_idx__"]
        )

        lookup_values = self.df[lookup_value_column] if lookup_value_column is not None else self.df[lookup_column]
        self.df[new_column_name] = lookup_values.map(lambda x: lookup_series.get(x, not_found_value))

    def coincidir_posicion(self, lookup_column: str, new_column_name: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Similar a COINCIDIR de Excel pero devolviendo posición relativa (1-based).
        """
        df_to_use = external_df if external_df is not None else self.df

        if lookup_column not in df_to_use.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")

        lookup_values = self.df[lookup_value_column] if lookup_value_column is not None else self.df[lookup_column]
        lookup_series = df_to_use[lookup_column]

        def get_position(value):
            mask = lookup_series.eq(value)
            if not mask.any():
                return not_found_value
            return int(np.flatnonzero(mask.to_numpy())[0] + 1)

        self.df[new_column_name] = lookup_values.map(get_position)

    def buscarx(
        self,
        lookup_column: str,
        return_column: str,
        new_column_name: str,
        lookup_value_column: Optional[str] = None,
        not_found_value: Union[str, int, float] = np.nan,
        external_df: Optional[pd.DataFrame] = None,
        match_mode: Literal["exact", "contains", "starts_with", "ends_with"] = "exact",
        search_mode: Literal["first", "last"] = "first",
    ) -> None:
        """
        Implementación estilo BUSCARX (XLOOKUP).

        - match_mode='exact' usa búsqueda exacta.
        - match_mode='contains'/'starts_with'/'ends_with' aplica comparación textual.
        - search_mode='first'/'last' define cuál coincidencia devolver si hay múltiples.
        """
        df_to_use = external_df if external_df is not None else self.df

        if lookup_column not in df_to_use.columns or return_column not in df_to_use.columns:
            raise ValueError("Las columnas especificadas no existen en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")

        lookup_values = self.df[lookup_value_column] if lookup_value_column is not None else self.df[lookup_column]

        if match_mode == "exact":
            keep_mode = "first" if search_mode == "first" else "last"
            lookup_series = (
                df_to_use[[lookup_column, return_column]]
                .drop_duplicates(subset=lookup_column, keep=keep_mode)
                .set_index(lookup_column)[return_column]
            )
            self.df[new_column_name] = lookup_values.map(lambda x: lookup_series.get(x, not_found_value))
            return

        lookup_text = df_to_use[lookup_column].astype(str)
        return_series = df_to_use[return_column]

        def resolve_match(value):
            value_text = str(value)
            if match_mode == "contains":
                mask = lookup_text.str.contains(value_text, na=False, regex=False)
            elif match_mode == "starts_with":
                mask = lookup_text.str.startswith(value_text, na=False)
            elif match_mode == "ends_with":
                mask = lookup_text.str.endswith(value_text, na=False)
            else:
                raise ValueError("'match_mode' no soportado.")

            matched = return_series[mask]
            if matched.empty:
                return not_found_value
            return matched.iloc[0] if search_mode == "first" else matched.iloc[-1]

        self.df[new_column_name] = lookup_values.map(resolve_match)

    def buscarx_multiple(
        self,
        lookup_column: str,
        return_columns: List[str],
        new_column_prefix: str,
        lookup_value_column: Optional[str] = None,
        not_found_value: Union[str, int, float] = np.nan,
        external_df: Optional[pd.DataFrame] = None,
        search_mode: Literal["first", "last"] = "first",
    ) -> None:
        """
        Variante de BUSCARX que devuelve múltiples columnas.
        """
        df_to_use = external_df if external_df is not None else self.df

        self._validate_columns([lookup_column])
        if lookup_column not in df_to_use.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame.")
        for col in return_columns:
            if col not in df_to_use.columns:
                raise ValueError(f"La columna '{col}' no existe en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")

        keep_mode = "first" if search_mode == "first" else "last"
        lookup_table = (
            df_to_use[[lookup_column] + return_columns]
            .drop_duplicates(subset=lookup_column, keep=keep_mode)
            .set_index(lookup_column)
        )

        lookup_values = self.df[lookup_value_column] if lookup_value_column is not None else self.df[lookup_column]

        for col in return_columns:
            mapping = lookup_table[col].to_dict()
            self.df[f"{new_column_prefix}_{col}"] = lookup_values.map(lambda x: mapping.get(x, not_found_value))

    def filtrar(self, conditions: List[Tuple[str, str, Union[str, int, float]]], logic: str = "and") -> pd.DataFrame:
        """
        Devuelve un DataFrame filtrado al estilo FILTER de Excel, sin modificar self.df.
        """
        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")

        condition_results = [
            self._evaluate_condition(condition_column, operator, value)
            for condition_column, operator, value in conditions
        ]
        final_condition = self._reduce_conditions(condition_results, logic)
        return self.df.loc[final_condition].copy()

    def extrae(self, column: str, start: int, length: int, new_column_name: str) -> None:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str[start-1:start-1+length]

    def derecha(self, column: str, num_chars: int, new_column_name: str) -> None:
        """
        Extrae los últimos `num_chars` caracteres de la columna `column` y los coloca en una nueva columna `new_column_name`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str[-num_chars:]

    def izquierda(self, column: str, num_chars: int, new_column_name: str) -> None:
        """
        Extrae los primeros `num_chars` caracteres de la columna `column` y los coloca en una nueva columna `new_column_name`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str[:num_chars]

    def dividir_texto(self, column: str, split_char: str, new_column_prefix: str, keep_split_char: bool = False, split_all: bool = True) -> None:
        """
        Divide el contenido de una columna en varias columnas en función de un carácter `split_char`.
        
        Parámetros:
        - column: La columna a dividir.
        - split_char: El carácter utilizado para dividir.
        - new_column_prefix: Prefijo para las nuevas columnas.
        - keep_split_char: Si se debe mantener el carácter que divide en las nuevas columnas (por defecto False).
        - split_all: Si se deben crear columnas por cada aparición del carácter de división (por defecto True).
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        
        split_df = self.df[column].str.split(split_char, expand=True)
        num_splits = split_df.shape[1] if split_all else 2
        for i in range(num_splits):
            self.df[f"{new_column_prefix}_{i+1}"] = split_df[i]
            if keep_split_char and i < num_splits - 1:
                self.df[f"{new_column_prefix}_{i+1}"] += split_char

    def encontrar(self, column: str, find_string: str, new_column_name: str) -> None:
        """
        Devuelve la posición de la primera aparición de una subcadena dentro de la cadena en la columna `column`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str.find(find_string)

    def hallar(self, column: str, find_string: str, new_column_name: str) -> None:
        """
        Alias de `encontrar`.
        """
        self.encontrar(column, find_string, new_column_name)

    def contiene_texto(self, column: str, pattern: str, new_column_name: str, case: bool = True) -> None:
        """
        Indica si cada valor de texto contiene el patrón especificado.
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.contains(pattern, case=case, na=False, regex=False)

    def empieza_con(self, column: str, prefix: str, new_column_name: str) -> None:
        """
        Indica si cada valor de texto empieza con el prefijo especificado.
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.startswith(prefix, na=False)

    def termina_con(self, column: str, suffix: str, new_column_name: str) -> None:
        """
        Indica si cada valor de texto termina con el sufijo especificado.
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.endswith(suffix, na=False)

    def nompropio(self, column: str, new_column_name: str) -> None:
        """
        Convierte la primera letra de cada palabra en mayúscula y el resto en minúscula en la columna `column`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str.title()

    def repetir(self, column: str, num_repeats: int, new_column_name: str) -> None:
        """
        Repite el contenido de la columna `column` `num_repeats` veces y lo coloca en una nueva columna `new_column_name`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column] * num_repeats

    def reemplazar(self, column: str, old_value: str, new_value: str, new_column_name: str) -> None:
        """
        Reemplaza todas las apariciones de `old_value` con `new_value` en la columna `column` y coloca el resultado en `new_column_name`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str.replace(old_value, new_value, regex=False)

    def concatenar_secuencia(self, sequence: List[str], new_column_name: str) -> None:
        """
        Concatena columnas y textos definidos por el usuario en una nueva columna.

        Parámetros:
        - sequence: Lista que contiene nombres de columnas y textos. Si es un nombre de columna, se especifica como un string.
                    Si es un texto o separador, se especifica como un string.
                    Ejemplo: ["Columna1", "-", "Columna2", "%%", "Columna3"]
        - new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        """
        if not sequence:
            raise ValueError("La secuencia no puede estar vacía.")
        
        concatenated_series = pd.Series([""] * len(self.df))

        for item in sequence:
            if item in self.df.columns:  # Si es una columna
                concatenated_series += self.df[item].astype(str)
            else:  # Si es un texto o separador
                concatenated_series += item

        self.df[new_column_name] = concatenated_series

    def espacios(self, column: str, new_column_name: str) -> None:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str.strip()

    def limpiar_texto(self, column: str, new_column_name: str) -> None:
        """
        Elimina caracteres no imprimibles de una columna de texto (similar a LIMPIAR en Excel).
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.replace(r"[\x00-\x1F\x7F]", "", regex=True)

    def si(self, conditions: List[Tuple[str, str, Union[str, int, float]]], true_value: Union[str, int, float, Callable, pd.Series], false_value: Union[str, int, float, Callable, pd.Series], new_column_name: str, all_conditions: bool = True, errors: Optional[Literal["raise", "coerce"]] = None) -> None:
        """
        Aplica una lógica condicional tipo SI (IF) a las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parámetros:
        - conditions: Lista de tuplas que contienen (columna, operador, valor). Ejemplo: [("columna1", "==", 10), ("columna2", ">", 5)]
        - true_value: Valor para condición verdadera. Soporta escalar, callable, Series o nombre de columna.
        - false_value: Valor para condición falsa. Soporta escalar, callable, Series o nombre de columna.
        - new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        - all_conditions: Si es True, todas las condiciones deben cumplirse. Si es False, al menos una condición debe cumplirse.

        Nota de rendimiento:
        - Si true_value/false_value es callable, se evalúa fila a fila (más lento).
        - Si usas escalar/Series/nombre de columna, se aplica de forma vectorizada.
        """
        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")

        condition_results = [
            self._evaluate_condition(condition_column, operator, value)
            for condition_column, operator, value in conditions
        ]
        final_condition = self._reduce_conditions(condition_results, "and" if all_conditions else "or")
        
        try:
            true_series = self._resolve_conditional_value(true_value)
            false_series = self._resolve_conditional_value(false_value)
            self.df[new_column_name] = np.where(final_condition, true_series, false_series)
        except Exception as error:
            self._handle_error(error, errors)
            self.df[new_column_name] = np.nan

    def si_conjunto(
        self,
        condition_groups: List[List[Tuple[str, str, Union[str, int, float]]]],
        result_values: List[Union[str, int, float, Callable, pd.Series]],
        false_value: Union[str, int, float, Callable, pd.Series],
        new_column_name: str,
        errors: Optional[Literal["raise", "coerce"]] = None,
    ) -> None:
        """
        Evalúa múltiples bloques de condiciones (en orden) y asigna el primer resultado cuya condición se cumpla.

        Cada bloque en condition_groups usa lógica AND entre sus condiciones.
        Si ningún bloque se cumple, se usa false_value.
        """
        if not condition_groups:
            raise ValueError("'condition_groups' no puede estar vacío.")
        self._validate_same_length(condition_groups, result_values)

        try:
            output_series = self._resolve_conditional_value(false_value)

            for group_conditions, group_result in reversed(list(zip(condition_groups, result_values))):
                if not group_conditions:
                    raise ValueError("Cada grupo de condiciones debe contener al menos una condición.")

                condition_results = [
                    self._evaluate_condition(condition_column, operator, value)
                    for condition_column, operator, value in group_conditions
                ]
                group_mask = self._reduce_conditions(condition_results, "and")
                group_result_series = self._resolve_conditional_value(group_result)

                output_series = pd.Series(
                    np.where(group_mask, group_result_series, output_series),
                    index=self.df.index,
                )

            self.df[new_column_name] = output_series
        except Exception as error:
            self._handle_error(error, errors)
            self.df[new_column_name] = np.nan

    def contar_si_agg(self, condition_column: str, condition_value: Union[str, int, float]) -> int:
        if condition_column not in self.df.columns:
            raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
        return int((self.df[condition_column] == condition_value).sum())

    def contar_si_col(self, condition_column: str, condition_value: Union[str, int, float], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_si_agg(condition_column, condition_value)

    def contar_si(self, condition_column: str, condition_value: Union[str, int, float]) -> int:
        """
        Alias de `contar_si_agg`.
        """
        return self.contar_si_agg(condition_column, condition_value)

    def contar_si_conjunto_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]]) -> int:
        if len(condition_columns) != len(condition_values):
            raise ValueError("Las listas de columnas y valores de condición deben tener la misma longitud.")
        for column in condition_columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        return int(condition_met.sum())

    def contar_si_conjunto_col(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_si_conjunto_agg(condition_columns, condition_values)

    def contar_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]]) -> int:
        """
        Alias de `contar_si_conjunto_agg`.
        """
        return self.contar_si_conjunto_agg(condition_columns, condition_values)

    def contar_si_rango_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float]) -> int:
        if condition_column not in self.df.columns:
            raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
        in_range = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        return int(in_range.sum())

    def contar_si_rango_col(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_si_rango_agg(condition_column, lower_bound, upper_bound)

    def contar_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float]) -> int:
        """
        Alias de `contar_si_rango_agg`.
        """
        return self.contar_si_rango_agg(condition_column, lower_bound, upper_bound)

    def sumar_si_rango_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], sum_columns: Union[str, List[str]]) -> float:
        """
        Devuelve una suma agregada condicionada por rango (SUMAR.SI con rango).
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_column(condition_column)
        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        row_sum = self.df[sum_columns_list].sum(axis=1)
        return float(row_sum[condition_met].sum())

    def sumar_si_rango_col_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        self.df[new_column_name] = self.sumar_si_rango_agg(condition_column, lower_bound, upper_bound, sum_columns)

    def sumar_si_rango_total(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], sum_columns: Union[str, List[str]]) -> float:
        """
        Alias de `sumar_si_rango_agg`.
        """
        return self.sumar_si_rango_agg(condition_column, lower_bound, upper_bound, sum_columns)

    def promedio_si_rango_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> float:
        """
        Devuelve un promedio agregado condicionado por rango (PROMEDIO.SI con rango).
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_columns([condition_column, target_column])
        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        filtered = self.df.loc[condition_met, target_column]
        return float(np.nan) if filtered.empty else float(filtered.mean())

    def promedio_si_rango_col_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.promedio_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def promedio_si_rango_total(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> float:
        """
        Alias de `promedio_si_rango_agg`.
        """
        return self.promedio_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def max_si_rango_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> Union[int, float]:
        """
        Devuelve el máximo de `target_column` condicionado por rango.
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_columns([condition_column, target_column])
        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        filtered = self.df.loc[condition_met, target_column]
        return np.nan if filtered.empty else filtered.max()

    def max_si_rango_col(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.max_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def max_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> Union[int, float]:
        """
        Alias de `max_si_rango_agg`.
        """
        return self.max_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def min_si_rango_agg(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> Union[int, float]:
        """
        Devuelve el mínimo de `target_column` condicionado por rango.
        """
        if lower_bound > upper_bound:
            raise ValueError("'lower_bound' no puede ser mayor que 'upper_bound'.")

        self._validate_columns([condition_column, target_column])
        condition_met = self.df[condition_column].between(lower_bound, upper_bound, inclusive="both")
        filtered = self.df.loc[condition_met, target_column]
        return np.nan if filtered.empty else filtered.min()

    def min_si_rango_col(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.min_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def min_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], target_column: str) -> Union[int, float]:
        """
        Alias de `min_si_rango_agg`.
        """
        return self.min_si_rango_agg(condition_column, lower_bound, upper_bound, target_column)

    def sumar_si_agg(self, condition_column: str, condition_value: Union[str, int, float], sum_columns: Union[str, List[str]]) -> float:
        """
        Devuelve una suma agregada condicionada (similar a SUMAR.SI), sobre una o varias columnas.
        """
        self._validate_column(condition_column)
        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_column] == condition_value
        row_sum = self.df[sum_columns_list].sum(axis=1)
        return float(row_sum[condition_met].sum())

    def sumar_si_col_agg(self, condition_column: str, condition_value: Union[str, int, float], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Escribe el resultado agregado de SUMAR.SI en una columna (valor repetido).
        """
        self.df[new_column_name] = self.sumar_si_agg(condition_column, condition_value, sum_columns)

    def sumar_si_total(self, condition_column: str, condition_value: Union[str, int, float], sum_columns: Union[str, List[str]]) -> float:
        """
        Alias de `sumar_si_agg`.
        """
        return self.sumar_si_agg(condition_column, condition_value, sum_columns)

    def promedio_si_agg(self, condition_column: str, condition_value: Union[str, int, float], target_column: str) -> float:
        """
        Devuelve un promedio agregado condicionado (similar a PROMEDIO.SI) sobre una columna objetivo.
        """
        self._validate_columns([condition_column, target_column])
        filtered = self.df.loc[self.df[condition_column] == condition_value, target_column]
        return float(np.nan) if filtered.empty else float(filtered.mean())

    def promedio_si_col_agg(self, condition_column: str, condition_value: Union[str, int, float], target_column: str, new_column_name: str) -> None:
        """
        Escribe el resultado agregado de PROMEDIO.SI en una columna (valor repetido).
        """
        self.df[new_column_name] = self.promedio_si_agg(condition_column, condition_value, target_column)

    def promedio_si_conjunto_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> float:
        """
        Devuelve un promedio agregado con múltiples condiciones (PROMEDIO.SI.CONJUNTO).
        """
        self._validate_same_length(condition_columns, condition_values)
        self._validate_columns(condition_columns + [target_column])

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        filtered = self.df.loc[condition_met, target_column]
        return float(np.nan) if filtered.empty else float(filtered.mean())

    def promedio_si_conjunto_col_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str, new_column_name: str) -> None:
        """
        Escribe el resultado agregado de PROMEDIO.SI.CONJUNTO en una columna (valor repetido).
        """
        self.df[new_column_name] = self.promedio_si_conjunto_agg(condition_columns, condition_values, target_column)

    def promedio_si_conjunto_total(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> float:
        """
        Alias de `promedio_si_conjunto_agg`.
        """
        return self.promedio_si_conjunto_agg(condition_columns, condition_values, target_column)

    def contar_si_conjunto_rango_agg(self, condition_columns: List[str], lower_bounds: List[Union[int, float]], upper_bounds: List[Union[int, float]]) -> int:
        self._validate_same_length(condition_columns, lower_bounds, upper_bounds)
        for column in condition_columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        lower_series = pd.Series(lower_bounds, index=condition_columns)
        upper_series = pd.Series(upper_bounds, index=condition_columns)
        values = self.df[condition_columns]
        condition_met = values.ge(lower_series).all(axis=1) & values.le(upper_series).all(axis=1)
        return int(condition_met.sum())

    def contar_si_conjunto_rango_col(self, condition_columns: List[str], lower_bounds: List[Union[int, float]], upper_bounds: List[Union[int, float]], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_si_conjunto_rango_agg(condition_columns, lower_bounds, upper_bounds)

    def contar_si_conjunto_rango(self, condition_columns: List[str], lower_bounds: List[Union[int, float]], upper_bounds: List[Union[int, float]]) -> int:
        """
        Alias de `contar_si_conjunto_rango_agg`.
        """
        return self.contar_si_conjunto_rango_agg(condition_columns, lower_bounds, upper_bounds)

    def sumar_si_conjunto_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], sum_columns: Union[str, List[str]]) -> float:
        """
        Devuelve una suma agregada con múltiples condiciones (similar a SUMAR.SI.CONJUNTO).
        """
        self._validate_same_length(condition_columns, condition_values)
        self._validate_columns(condition_columns)

        sum_columns_list = self._as_column_list(sum_columns)
        self._validate_columns(sum_columns_list)

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        row_sum = self.df[sum_columns_list].sum(axis=1)
        return float(row_sum[condition_met].sum())

    def sumar_si_conjunto_col_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], sum_columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Escribe el resultado agregado de SUMAR.SI.CONJUNTO en una columna (valor repetido).
        """
        self.df[new_column_name] = self.sumar_si_conjunto_agg(condition_columns, condition_values, sum_columns)

    def max_si_agg(self, condition_column: str, condition_value: Union[str, int, float], target_column: str) -> Union[int, float]:
        """
        Devuelve el máximo de `target_column` para las filas que cumplen la condición.
        """
        self._validate_columns([condition_column, target_column])
        filtered = self.df.loc[self.df[condition_column] == condition_value, target_column]
        return np.nan if filtered.empty else filtered.max()

    def max_si_col(self, condition_column: str, condition_value: Union[str, int, float], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.max_si_agg(condition_column, condition_value, target_column)

    def max_si(self, condition_column: str, condition_value: Union[str, int, float], target_column: str) -> Union[int, float]:
        """
        Alias de `max_si_agg`.
        """
        return self.max_si_agg(condition_column, condition_value, target_column)

    def min_si_agg(self, condition_column: str, condition_value: Union[str, int, float], target_column: str) -> Union[int, float]:
        """
        Devuelve el mínimo de `target_column` para las filas que cumplen la condición.
        """
        self._validate_columns([condition_column, target_column])
        filtered = self.df.loc[self.df[condition_column] == condition_value, target_column]
        return np.nan if filtered.empty else filtered.min()

    def min_si_col(self, condition_column: str, condition_value: Union[str, int, float], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.min_si_agg(condition_column, condition_value, target_column)

    def min_si(self, condition_column: str, condition_value: Union[str, int, float], target_column: str) -> Union[int, float]:
        """
        Alias de `min_si_agg`.
        """
        return self.min_si_agg(condition_column, condition_value, target_column)

    def max_si_conjunto_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> Union[int, float]:
        """
        Devuelve el máximo de `target_column` para filas que cumplen múltiples condiciones.
        """
        self._validate_same_length(condition_columns, condition_values)
        self._validate_columns(condition_columns + [target_column])

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        filtered = self.df.loc[condition_met, target_column]
        return np.nan if filtered.empty else filtered.max()

    def max_si_conjunto_col(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.max_si_conjunto_agg(condition_columns, condition_values, target_column)

    def max_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> Union[int, float]:
        """
        Alias de `max_si_conjunto_agg`.
        """
        return self.max_si_conjunto_agg(condition_columns, condition_values, target_column)

    def min_si_conjunto_agg(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> Union[int, float]:
        """
        Devuelve el mínimo de `target_column` para filas que cumplen múltiples condiciones.
        """
        self._validate_same_length(condition_columns, condition_values)
        self._validate_columns(condition_columns + [target_column])

        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        filtered = self.df.loc[condition_met, target_column]
        return np.nan if filtered.empty else filtered.min()

    def min_si_conjunto_col(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.min_si_conjunto_agg(condition_columns, condition_values, target_column)

    def min_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], target_column: str) -> Union[int, float]:
        """
        Alias de `min_si_conjunto_agg`.
        """
        return self.min_si_conjunto_agg(condition_columns, condition_values, target_column)

    def sumaproducto_agg(self, columns: List[str]) -> float:
        """
        Calcula SUMAPRODUCTO al estilo Excel: suma de los productos fila a fila.
        """
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        return float(self.df[columns].product(axis=1).sum())

    def sumaproducto_col(self, columns: List[str], new_column_name: str) -> None:
        """
        Escribe el resultado agregado de SUMAPRODUCTO en una columna (valor repetido).
        """
        self.df[new_column_name] = self.sumaproducto_agg(columns)

    def sumaproducto(self, columns: List[str]) -> float:
        """
        Alias de `sumaproducto_agg`.
        """
        return self.sumaproducto_agg(columns)

    def sumar(self, columns: List[str], new_column_name: str) -> None:
        """
        Suma los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        self.df[new_column_name] = self.df[columns].sum(axis=1)

    def restar(self, columns: List[str], new_column_name: str) -> None:
        """
        Resta los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        values = self.df[columns]
        result = values.iloc[:, 0]
        for column in columns[1:]:
            result = result - self.df[column]
        self.df[new_column_name] = result

    def multiplicar(self, columns: List[str], new_column_name: str) -> None:
        """
        Multiplica los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        self.df[new_column_name] = self.df[columns].product(axis=1)

    def dividir_aritmetica(self, columns: List[str], new_column_name: str) -> None:
        """
        Divide los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        values = self.df[columns]
        result = values.iloc[:, 0]
        for column in columns[1:]:
            divisor = self.df[column].replace(0, np.nan)
            result = result / divisor
        self.df[new_column_name] = result

    def dividir(self, *args, **kwargs) -> None:
        """
        Método compatible para dividir texto o dividir aritméticamente.

        Formatos soportados:
        - dividir(column: str, split_char: str, new_column_prefix: str, keep_split_char: bool = False, split_all: bool = True)
        - dividir(columns: List[str], new_column_name: str)
        """
        if len(args) >= 2 and isinstance(args[0], list):
            columns = args[0]
            new_column_name = args[1]
            return self.dividir_aritmetica(columns, new_column_name)

        if len(args) >= 3 and isinstance(args[0], str) and isinstance(args[1], str) and isinstance(args[2], str):
            column = args[0]
            split_char = args[1]
            new_column_prefix = args[2]
            keep_split_char = kwargs.get("keep_split_char", False)
            split_all = kwargs.get("split_all", True)
            return self.dividir_texto(column, split_char, new_column_prefix, keep_split_char, split_all)

        raise ValueError(
            "Uso inválido de 'dividir'. Usa (columns: List[str], new_column_name: str) o "
            "(column: str, split_char: str, new_column_prefix: str, keep_split_char=False, split_all=True)."
        )

    def promedio(self, columns: List[str], new_column_name: str) -> None:
        """
        Calcula el promedio de los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        self.df[new_column_name] = self.df[columns].mean(axis=1)

    def mediana(self, columns: List[str], new_column_name: str) -> None:
        """
        Calcula la mediana de los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self._validate_columns(columns)
        self.df[new_column_name] = self.df[columns].median(axis=1)

    def y_o(self, conditions: List[Tuple[str, str, Union[str, int, float]]], logic: str, new_column_name: str) -> None:
        """
        Aplica una lógica condicional (AND/OR) a las filas del DataFrame y agrega una nueva columna con los resultados.

        Parameters:
        - conditions: Lista de tuplas con (nombre_columna, operador, valor).
        - logic: 'and' para aplicar lógica AND, 'or' para aplicar lógica OR.
        - new_column_name: Nombre de la nueva columna.
        """
        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")

        condition_results = [
            self._evaluate_condition(condition_column, operator, value)
            for condition_column, operator, value in conditions
        ]
        self.df[new_column_name] = self._reduce_conditions(condition_results, logic)
        
    def contar_valores_agg(self, columns: List[str]) -> pd.Series:
        self._validate_columns(columns)
        return self.df[columns].nunique(axis=1)

    def contar_valores_col(self, columns: List[str], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_valores_agg(columns)

    def contar_valores(self, columns: List[str]) -> pd.Series:
        """
        Alias de `contar_valores_agg`.
        """
        return self.contar_valores_agg(columns)

    def contar_valores_unicos_agg(self, columns: List[str]) -> pd.Series:
        return self.contar_valores_agg(columns)

    def contar_valores_unicos_col(self, columns: List[str], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_valores_unicos_agg(columns)

    def contar_valores_unicos(self, columns: List[str]) -> pd.Series:
        """
        Alias de `contar_valores_unicos_agg`.
        """
        return self.contar_valores_unicos_agg(columns)

    def contar_valores_duplicados_agg(self, columns: List[str]) -> int:
        self._validate_columns(columns)
        return int(self.df[columns].duplicated(keep=False).sum())

    def contar_valores_duplicados_col(self, columns: List[str], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_valores_duplicados_agg(columns)

    def contar_valores_duplicados(self, columns: List[str]) -> int:
        """
        Alias de `contar_valores_duplicados_agg`.
        """
        return self.contar_valores_duplicados_agg(columns)

    def contar_valores_unicos_duplicados_agg(self, columns: List[str]) -> int:
        self._validate_columns(columns)
        return int(self.df[columns].duplicated(keep='first').sum())

    def contar_valores_unicos_duplicados_col(self, columns: List[str], new_column_name: str) -> None:
        self.df[new_column_name] = self.contar_valores_unicos_duplicados_agg(columns)

    def contar_valores_unicos_duplicados(self, columns: List[str]) -> int:
        """
        Alias de `contar_valores_unicos_duplicados_agg`.
        """
        return self.contar_valores_unicos_duplicados_agg(columns)

    def transponer(self) -> None:
        """
        Transpone el DataFrame, intercambiando filas y columnas.
        """
        self.df = self.df.T

    def hoy(self, new_column_name: str) -> None:
        """
        Agrega la fecha actual a una nueva columna en formato 'YYYY-MM-DD'.
        """
        self.df[new_column_name] = pd.Timestamp.now().date()

    def ahora(self, new_column_name: str) -> None:
        """
        Agrega la fecha y hora actual a una nueva columna en formato 'YYYY-MM-DD HH:MM:SS'.
        """
        self.df[new_column_name] = pd.Timestamp.now()

    def rellenar(self, column: str, fill_value: Union[str, int, float], new_column_name: Optional[str] = None) -> None:
        """
        Rellena los valores faltantes en la columna `column` con `fill_value`. Si se proporciona `new_column_name`, 
        coloca el resultado en una nueva columna con ese nombre; de lo contrario, actualiza la columna original.

        Parameters:
        - column: Nombre de la columna a rellenar.
        - fill_value: Valor con el que rellenar los valores faltantes.
        - new_column_name: Nombre de la nueva columna (opcional).
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    
        if new_column_name:
            self.df[new_column_name] = self.df[column].fillna(fill_value)
        else:
            self.df[column] = self.df[column].fillna(fill_value)

    def rellenar_hacia_adelante(self, column: str, new_column_name: Optional[str] = None) -> None:
        """
        Rellena nulos hacia adelante (forward fill) en una columna.
        """
        self._validate_column(column)
        filled = self.df[column].ffill()
        if new_column_name:
            self.df[new_column_name] = filled
        else:
            self.df[column] = filled

    def rellenar_hacia_atras(self, column: str, new_column_name: Optional[str] = None) -> None:
        """
        Rellena nulos hacia atrás (backward fill) en una columna.
        """
        self._validate_column(column)
        filled = self.df[column].bfill()
        if new_column_name:
            self.df[new_column_name] = filled
        else:
            self.df[column] = filled

    def coalesce(self, columns: Union[str, List[str]], new_column_name: str) -> None:
        """
        Devuelve el primer valor no nulo entre varias columnas (por fila).
        """
        columns_list = self._as_column_list(columns)
        self._validate_columns(columns_list)
        self.df[new_column_name] = self.df[columns_list].bfill(axis=1).iloc[:, 0]

    def ordenar_por(self, columns: Union[str, List[str]], ascending: Union[bool, List[bool]] = True, reset_index: bool = False) -> None:
        """
        Ordena el DataFrame por una o varias columnas.
        """
        columns_list = self._as_column_list(columns)
        self._validate_columns(columns_list)
        self.df = self.df.sort_values(by=columns_list, ascending=ascending)
        if reset_index:
            self.df = self.df.reset_index(drop=True)

    def renombrar_columnas(self, rename_map: dict) -> None:
        """
        Renombra columnas del DataFrame según un diccionario {columna_actual: columna_nueva}.
        """
        if not isinstance(rename_map, dict) or not rename_map:
            raise ValueError("'rename_map' debe ser un diccionario no vacío.")
        self._validate_columns(list(rename_map.keys()))
        self.df = self.df.rename(columns=rename_map)

    def hipervinculo(self, url_column: str, display_text_column: str, new_column_name: Optional[str] = None) -> None:
        """
        Crea una nueva columna con hipervínculos HTML basados en las columnas de URL y texto de visualización.
        Si no se proporciona `new_column_name`, actualiza la columna de URL con los hipervínculos.

        Parameters:
        - url_column: Nombre de la columna que contiene las URLs.
        - display_text_column: Nombre de la columna que contiene el texto de visualización.
        - new_column_name: Nombre de la nueva columna que contendrá los hipervínculos (opcional).
        """
        if url_column not in self.df.columns:
            raise ValueError(f"La columna '{url_column}' no existe en el DataFrame.")
        if display_text_column not in self.df.columns:
            raise ValueError(f"La columna '{display_text_column}' no existe en el DataFrame.")

        escaped_url = self.df[url_column].map(lambda value: html.escape(str(value)))
        escaped_text = self.df[display_text_column].map(lambda value: html.escape(str(value)))
        hyperlink_series = '<a href="' + escaped_url + '">' + escaped_text + '</a>'
        
        if new_column_name:
            self.df[new_column_name] = hyperlink_series
        else:
            self.df[url_column] = hyperlink_series

    def buscarv_multiple(self, lookup_column: str, return_columns: List[str], new_column_prefix: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Variante de BUSCARV que permite devolver múltiples columnas en lugar de solo una.
        
        Parámetros:
        - lookup_column: La columna en la que se buscará el valor.
        - return_columns: Lista de columnas de las cuales se devolverán los valores.
        - new_column_prefix: Prefijo para las nuevas columnas que se agregarán.
        - lookup_value_column: (Opcional) Nombre de la columna que contiene los valores a buscar.
        - not_found_value: Valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        - external_df: (Opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df

        if lookup_column not in df_to_use.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame.")
        if lookup_value_column is not None and lookup_value_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
        if lookup_value_column is None and lookup_column not in self.df.columns:
            raise ValueError(f"La columna '{lookup_column}' no existe en el DataFrame de trabajo.")
        
        for col in return_columns:
            if col not in df_to_use.columns:
                raise ValueError(f"La columna '{col}' no existe en el DataFrame.")

        lookup_series = (
            df_to_use[[lookup_column] + return_columns]
            .drop_duplicates(subset=lookup_column, keep="first")
            .set_index(lookup_column)[return_columns]
        )
        lookup_dict_by_column = {
            col: lookup_series[col].to_dict()
            for col in return_columns
        }

        lookup_values = self.df[lookup_value_column] if lookup_value_column is not None else self.df[lookup_column]

        for col in return_columns:
            mapping = lookup_dict_by_column[col]
            self.df[f"{new_column_prefix}_{col}"] = lookup_values.map(lambda x: mapping.get(x, not_found_value))

    def reemplazar_multiple(self, column: str, replacements: dict, new_column_name: Optional[str] = None) -> None:
        """
        Reemplaza múltiples valores en una columna usando un diccionario de reemplazos.
        
        Parámetros:
        - column: Nombre de la columna donde se aplicarán los reemplazos.
        - replacements: Diccionario donde las claves son los valores originales y los valores son los nuevos valores.
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        if new_column_name:
            self.df[new_column_name] = self.df[column].replace(replacements)
        else:
            self.df[column] = self.df[column].replace(replacements)

    def extraer_numeros(self, column: str, new_column_name: str) -> None:
        """
        Extrae solo los números de una columna de texto y los almacena en una nueva columna.
        
        Parámetros:
        - column: Nombre de la columna de la que se extraerán los números.
        - new_column_name: Nombre de la nueva columna con los valores numéricos extraídos.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        
        self.df[new_column_name] = self.df[column].astype(str).str.extract(r'(\d+)')

    def eliminar_caracteres(self, column: str, characters: str, new_column_name: Optional[str] = None) -> None:
        """
        Elimina caracteres específicos de una columna de texto.
        
        Parámetros:
        - column: Nombre de la columna a modificar.
        - characters: Cadena de caracteres que serán eliminados.
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        regex_pattern = f"[{re.escape(characters)}]"
        if new_column_name:
            self.df[new_column_name] = self.df[column].str.replace(regex_pattern, '', regex=True)
        else:
            self.df[column] = self.df[column].str.replace(regex_pattern, '', regex=True)

    def redondear(self, column: str, decimals: int, new_column_name: Optional[str] = None) -> None:
        """
        Redondea los valores de una columna numérica a un número específico de decimales.
        
        Parámetros:
        - column: Nombre de la columna a redondear.
        - decimals: Número de decimales a los que redondear.
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        if new_column_name:
            self.df[new_column_name] = self.df[column].round(decimals)
        else:
            self.df[column] = self.df[column].round(decimals)

    def redondear_mas(self, column: str, decimals: int, new_column_name: str) -> None:
        """
        Redondea hacia arriba (ROUNDUP de Excel) a una cantidad de decimales.
        """
        self._validate_column(column)
        factor = 10 ** decimals
        self.df[new_column_name] = self.df[column].apply(
            lambda x: np.ceil(x * factor) / factor if pd.notnull(x) else x
        )

    def redondear_menos(self, column: str, decimals: int, new_column_name: str) -> None:
        """
        Redondea hacia abajo (ROUNDDOWN de Excel) a una cantidad de decimales.
        """
        self._validate_column(column)
        factor = 10 ** decimals
        self.df[new_column_name] = self.df[column].apply(
            lambda x: np.floor(x * factor) / factor if pd.notnull(x) else x
        )

    def entero(self, column: str, new_column_name: str) -> None:
        """
        Devuelve la parte entera redondeada hacia abajo (INT de Excel).
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].apply(lambda x: np.floor(x) if pd.notnull(x) else x)

    def pago(
        self,
        tasa: float,
        periodos: int,
        valor_actual: float,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Calcula el pago periódico (PMT) de un préstamo o inversión.
        """
        if periodos <= 0:
            raise ValueError("'periodos' debe ser mayor que 0.")
        if tipo not in {"end", "begin"}:
            raise ValueError("'tipo' debe ser 'end' o 'begin'.")

        when = 1 if tipo == "begin" else 0

        if tasa == 0:
            result = -(valor_actual + valor_futuro) / periodos
        else:
            growth = (1 + tasa) ** periodos
            denominator = (1 + tasa * when) * (growth - 1)
            if denominator == 0:
                raise ValueError("No se puede calcular el pago con los parámetros proporcionados.")
            result = -(tasa * (valor_futuro + valor_actual * growth)) / denominator

        result = float(result)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    def valor_futuro(
        self,
        tasa: float,
        periodos: int,
        pago: float = 0.0,
        valor_actual: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Calcula el valor futuro (FV) de una inversión.
        """
        if periodos < 0:
            raise ValueError("'periodos' no puede ser negativo.")
        if tipo not in {"end", "begin"}:
            raise ValueError("'tipo' debe ser 'end' o 'begin'.")

        when = 1 if tipo == "begin" else 0
        if tasa == 0:
            result = -(valor_actual + pago * periodos)
        else:
            growth = (1 + tasa) ** periodos
            annuity_term = ((1 + tasa * when) * (growth - 1)) / tasa
            result = -(valor_actual * growth + pago * annuity_term)

        result = float(result)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    def valor_presente(
        self,
        tasa: float,
        periodos: int,
        pago: float = 0.0,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Calcula el valor presente (PV) de una inversión.
        """
        if periodos < 0:
            raise ValueError("'periodos' no puede ser negativo.")
        if tipo not in {"end", "begin"}:
            raise ValueError("'tipo' debe ser 'end' o 'begin'.")

        when = 1 if tipo == "begin" else 0
        if tasa == 0:
            result = -(valor_futuro + pago * periodos)
        else:
            growth = (1 + tasa) ** periodos
            annuity_term = ((1 + tasa * when) * (growth - 1)) / tasa
            result = -(valor_futuro + pago * annuity_term) / growth

        result = float(result)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    def periodos_pago(
        self,
        tasa: float,
        pago: float,
        valor_actual: float,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Calcula el número de periodos (NPER) necesario.
        """
        if pago == 0:
            raise ValueError("'pago' no puede ser 0.")
        if tipo not in {"end", "begin"}:
            raise ValueError("'tipo' debe ser 'end' o 'begin'.")

        when = 1 if tipo == "begin" else 0

        if tasa == 0:
            result = -(valor_actual + valor_futuro) / pago
        else:
            numerator = pago * (1 + tasa * when) - valor_futuro * tasa
            denominator = pago * (1 + tasa * when) + valor_actual * tasa
            if numerator <= 0 or denominator <= 0:
                raise ValueError("No se puede calcular NPER con los parámetros proporcionados.")
            result = np.log(numerator / denominator) / np.log(1 + tasa)

        result = float(result)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    def pmt(
        self,
        tasa: float,
        periodos: int,
        valor_actual: float,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Alias de `pago`.
        """
        return self.pago(tasa, periodos, valor_actual, valor_futuro, tipo, new_column_name)

    def fv(
        self,
        tasa: float,
        periodos: int,
        pago: float = 0.0,
        valor_actual: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Alias de `valor_futuro`.
        """
        return self.valor_futuro(tasa, periodos, pago, valor_actual, tipo, new_column_name)

    def pv(
        self,
        tasa: float,
        periodos: int,
        pago: float = 0.0,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Alias de `valor_presente`.
        """
        return self.valor_presente(tasa, periodos, pago, valor_futuro, tipo, new_column_name)

    def nper(
        self,
        tasa: float,
        pago: float,
        valor_actual: float,
        valor_futuro: float = 0.0,
        tipo: Literal["end", "begin"] = "end",
        new_column_name: Optional[str] = None,
    ) -> float:
        """
        Alias de `periodos_pago`.
        """
        return self.periodos_pago(tasa, pago, valor_actual, valor_futuro, tipo, new_column_name)

    def diferencia(self, column: str, new_column_name: str, periods: int = 1) -> None:
        """
        Calcula la diferencia entre el valor actual y el de N periodos anteriores.
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].diff(periods=periods)

    def porcentaje_cambio(self, column: str, new_column_name: str, periods: int = 1) -> None:
        """
        Calcula el cambio porcentual respecto al valor de N periodos anteriores.
        """
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].pct_change(periods=periods)

    def formatear_monedas(self, column: str, currency_symbol: str = "$", decimal_places: int = 2, new_column_name: Optional[str] = None) -> None:
        """
        Aplica formato de moneda a los valores de una columna numérica.
        
        Parámetros:
        - column: Nombre de la columna a formatear.
        - currency_symbol: Símbolo de moneda (por defecto '$').
        - decimal_places: Número de decimales a mostrar (por defecto 2).
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        formatted_series = self.df[column].apply(lambda x: f"{currency_symbol}{x:,.{decimal_places}f}" if pd.notnull(x) else x)

        if new_column_name:
            self.df[new_column_name] = formatted_series
        else:
            self.df[column] = formatted_series

    def fecha_a_texto(self, column: str, format_string: str = "%d/%m/%Y", new_column_name: Optional[str] = None) -> None:
        """
        Convierte una columna de fechas en texto con un formato específico.
        
        Parámetros:
        - column: Nombre de la columna con valores de fecha.
        - format_string: Formato en que se representará la fecha (por defecto 'DD/MM/YYYY').
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        datetime_series = self._to_datetime_series(column)

        if new_column_name:
            self.df[new_column_name] = datetime_series.dt.strftime(format_string)
        else:
            self.df[column] = datetime_series.dt.strftime(format_string)

    def texto_a_fecha(
        self,
        column: str,
        format_string: Optional[str] = "%d/%m/%Y",
        new_column_name: Optional[str] = None,
        dayfirst: bool = False,
        yearfirst: bool = False,
        errors: Optional[Literal["raise", "coerce"]] = None,
    ) -> None:
        """
        Convierte una columna de texto en fechas con un formato específico.
        
        Parámetros:
        - column: Nombre de la columna con valores de texto.
        - format_string: Formato en que se representará la fecha (por defecto 'DD/MM/YYYY').
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        datetime_series = self._to_datetime_series(
            column,
            format_string=format_string,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
            errors=errors,
        )

        if new_column_name:
            self.df[new_column_name] = datetime_series
        else:
            self.df[column] = datetime_series

    def diferencia_fechas(self, start_column: str, end_column: str, unit: str = "days", new_column_name: str = "Diferencia") -> None:
        """
        Calcula la diferencia entre dos columnas de fechas en la unidad especificada (días, horas, minutos, segundos).
        
        Parámetros:
        - start_column: Nombre de la columna con la fecha de inicio.
        - end_column: Nombre de la columna con la fecha final.
        - unit: Unidad de medida para la diferencia ('days', 'hours', 'minutes', 'seconds').
        - new_column_name: Nombre de la nueva columna que almacenará la diferencia de fechas.
        """
        self._validate_columns([start_column, end_column])

        start_series = self._to_datetime_series(start_column)
        end_series = self._to_datetime_series(end_column)
        delta = end_series - start_series

        if unit == "days":
            self.df[new_column_name] = delta.dt.days
        elif unit == "hours":
            self.df[new_column_name] = delta.dt.total_seconds() / 3600
        elif unit == "minutes":
            self.df[new_column_name] = delta.dt.total_seconds() / 60
        elif unit == "seconds":
            self.df[new_column_name] = delta.dt.total_seconds()
        else:
            raise ValueError("Unidad no válida. Usa 'days', 'hours', 'minutes' o 'seconds'.")

    def marcar_como_componente_fecha(
        self,
        column: str,
        component: Literal["year", "month", "day", "hour", "minute", "second", "ano", "año", "mes", "dia", "día", "hora", "minuto", "segundo"],
        new_column_name: Optional[str] = None,
        errors: Optional[Literal["raise", "coerce"]] = None,
    ) -> None:
        """
        Interpreta una columna como componente de fecha/hora (mes, día, año, hora, etc.)
        y valida su rango para que quede explícito para Python.
        """
        self._validate_column(column)

        component_map = {
            "year": "year",
            "ano": "year",
            "año": "year",
            "month": "month",
            "mes": "month",
            "day": "day",
            "dia": "day",
            "día": "day",
            "hour": "hour",
            "hora": "hour",
            "minute": "minute",
            "minuto": "minute",
            "second": "second",
            "segundo": "second",
        }
        normalized_component = component_map.get(component.lower())
        if normalized_component is None:
            raise ValueError("Componente no soportado. Usa year/month/day/hour/minute/second (o sus alias en español).")

        numeric = pd.to_numeric(self.df[column], errors="coerce")
        ranges = {
            "month": (1, 12),
            "day": (1, 31),
            "hour": (0, 23),
            "minute": (0, 59),
            "second": (0, 59),
            "year": (1, 9999),
        }
        min_value, max_value = ranges[normalized_component]
        in_range = numeric.between(min_value, max_value) | numeric.isna()

        mode = errors if errors is not None else self.errors
        if mode == "raise" and not in_range.all():
            raise ValueError(
                f"Se encontraron valores fuera de rango para '{normalized_component}'. "
                f"Rango permitido: [{min_value}, {max_value}]."
            )

        normalized = numeric.where(in_range, np.nan).round().astype("Int64")
        if new_column_name:
            self.df[new_column_name] = normalized
        else:
            self.df[column] = normalized

    def construir_fecha_desde_partes(
        self,
        year_column: str,
        month_column: str,
        day_column: str,
        new_column_name: str,
        hour_column: Optional[str] = None,
        minute_column: Optional[str] = None,
        second_column: Optional[str] = None,
        errors: Optional[Literal["raise", "coerce"]] = None,
    ) -> None:
        """
        Construye una fecha/hora a partir de columnas de componentes (año, mes, día, hora, minuto, segundo).
        """
        required_columns = [year_column, month_column, day_column]
        optional_columns = [hour_column, minute_column, second_column]
        self._validate_columns(required_columns + [column for column in optional_columns if column is not None])

        mode = errors if errors is not None else self.errors
        parse_errors = "raise" if mode == "raise" else "coerce"

        year_values = pd.to_numeric(self.df[year_column], errors="coerce")
        month_values = pd.to_numeric(self.df[month_column], errors="coerce")
        day_values = pd.to_numeric(self.df[day_column], errors="coerce")
        hour_values = pd.to_numeric(self.df[hour_column], errors="coerce") if hour_column else 0
        minute_values = pd.to_numeric(self.df[minute_column], errors="coerce") if minute_column else 0
        second_values = pd.to_numeric(self.df[second_column], errors="coerce") if second_column else 0

        datetime_df = pd.DataFrame(
            {
                "year": year_values,
                "month": month_values,
                "day": day_values,
                "hour": hour_values,
                "minute": minute_values,
                "second": second_values,
            },
            index=self.df.index,
        )

        self.df[new_column_name] = pd.to_datetime(datetime_df, errors=parse_errors)

    def dia_del_mes(self, column: str, new_column_name: str) -> None:
        """
        Extrae el día del mes (1-31) de una columna de fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.day

    def hora_del_dia(self, column: str, new_column_name: str) -> None:
        """
        Extrae la hora (0-23) de una columna de fecha/hora.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.hour

    def minuto(self, column: str, new_column_name: str) -> None:
        """
        Extrae el minuto (0-59) de una columna de fecha/hora.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.minute

    def segundo(self, column: str, new_column_name: str) -> None:
        """
        Extrae el segundo (0-59) de una columna de fecha/hora.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.second

    def semana_del_ano(self, column: str, new_column_name: str) -> None:
        """
        Extrae la semana ISO del año.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.isocalendar().week.astype("Int64")

    def trimestre(self, column: str, new_column_name: str) -> None:
        """
        Extrae el trimestre (1-4) de una columna de fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.quarter

    def nombre_mes(self, column: str, new_column_name: str, locale: Optional[str] = None) -> None:
        """
        Devuelve el nombre del mes de una columna de fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.month_name(locale=locale)

    def nombre_dia(self, column: str, new_column_name: str, locale: Optional[str] = None) -> None:
        """
        Devuelve el nombre del día de la semana de una columna de fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.day_name(locale=locale)

    def es_fin_de_semana(self, column: str, new_column_name: str) -> None:
        """
        Indica si la fecha corresponde a fin de semana.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.dayofweek >= 5

    def periodo_anio_mes(self, column: str, new_column_name: str, format_string: str = "%Y-%m") -> None:
        """
        Extrae el período año-mes de una columna de fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.strftime(format_string)

    def inicio_mes(self, column: str, new_column_name: str) -> None:
        """
        Devuelve el primer día del mes para cada fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.to_period("M").dt.to_timestamp()

    def fin_mes(self, column: str, new_column_name: str) -> None:
        """
        Devuelve el último día del mes para cada fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.to_period("M").dt.to_timestamp(how="end").dt.normalize()

    def inicio_trimestre(self, column: str, new_column_name: str) -> None:
        """
        Devuelve el primer día del trimestre para cada fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.to_period("Q").dt.start_time

    def fin_trimestre(self, column: str, new_column_name: str) -> None:
        """
        Devuelve el último día del trimestre para cada fecha.
        """
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.to_period("Q").dt.end_time.dt.normalize()

    def es_dia_habil(self, column: str, new_column_name: str, holidays: Optional[List[Union[str, pd.Timestamp]]] = None) -> None:
        """
        Indica si cada fecha corresponde a un día hábil (lunes-viernes), excluyendo feriados opcionales.
        """
        datetime_series = self._to_datetime_series(column, errors="coerce")
        days = datetime_series.dt.normalize().to_numpy(dtype="datetime64[D]")
        holiday_array = self._normalize_holidays(holidays)

        valid_mask = ~pd.isna(datetime_series)
        result = pd.Series([False] * len(self.df), index=self.df.index)
        if valid_mask.any():
            if holiday_array is None:
                result.loc[valid_mask] = np.is_busday(days[valid_mask.to_numpy()])
            else:
                result.loc[valid_mask] = np.is_busday(days[valid_mask.to_numpy()], holidays=holiday_array)

        self.df[new_column_name] = result

    def dias_habiles_entre(
        self,
        start_column: str,
        end_column: str,
        new_column_name: str,
        holidays: Optional[List[Union[str, pd.Timestamp]]] = None,
    ) -> None:
        """
        Calcula los días hábiles entre dos fechas por fila (fin excluido, como np.busday_count).
        """
        self._validate_columns([start_column, end_column])
        start_series = self._to_datetime_series(start_column, errors="coerce")
        end_series = self._to_datetime_series(end_column, errors="coerce")

        start_days = start_series.dt.normalize().to_numpy(dtype="datetime64[D]")
        end_days = end_series.dt.normalize().to_numpy(dtype="datetime64[D]")
        holiday_array = self._normalize_holidays(holidays)

        valid_mask = ~(start_series.isna() | end_series.isna())
        result = pd.Series(np.nan, index=self.df.index)
        if valid_mask.any():
            start_valid = start_days[valid_mask.to_numpy()]
            end_valid = end_days[valid_mask.to_numpy()]
            if holiday_array is None:
                counts = np.busday_count(start_valid, end_valid)
            else:
                counts = np.busday_count(start_valid, end_valid, holidays=holiday_array)
            result.loc[valid_mask] = counts

        self.df[new_column_name] = result

    def sumar_dias_habiles(
        self,
        column: str,
        business_days: int,
        new_column_name: str,
        holidays: Optional[List[Union[str, pd.Timestamp]]] = None,
    ) -> None:
        """
        Suma (o resta) días hábiles a una fecha por fila.
        """
        datetime_series = self._to_datetime_series(column, errors="coerce")
        base_days = datetime_series.dt.normalize().to_numpy(dtype="datetime64[D]")
        holiday_array = self._normalize_holidays(holidays)

        valid_mask = ~datetime_series.isna()
        result = pd.Series(pd.NaT, index=self.df.index, dtype="datetime64[ns]")

        if valid_mask.any():
            valid_days = base_days[valid_mask.to_numpy()]
            if holiday_array is None:
                shifted = np.busday_offset(valid_days, business_days, roll="forward")
            else:
                shifted = np.busday_offset(valid_days, business_days, roll="forward", holidays=holiday_array)
            result.loc[valid_mask] = pd.to_datetime(shifted)

        self.df[new_column_name] = result

    def dias_laborables_entre(
        self,
        start_column: str,
        end_column: str,
        new_column_name: str,
        holidays: Optional[List[Union[str, pd.Timestamp]]] = None,
    ) -> None:
        """
        Alias de `dias_habiles_entre` para compatibilidad de naming tipo Excel.
        """
        self.dias_habiles_entre(start_column, end_column, new_column_name, holidays=holidays)

    def dia_laborable(
        self,
        column: str,
        business_days: int,
        new_column_name: str,
        holidays: Optional[List[Union[str, pd.Timestamp]]] = None,
    ) -> None:
        """
        Alias de `sumar_dias_habiles` para compatibilidad tipo WORKDAY.
        """
        self.sumar_dias_habiles(column, business_days, new_column_name, holidays=holidays)

    def agrupar_transformar(
        self,
        group_columns: Union[str, List[str]],
        target_column: str,
        operation: Literal["sum", "mean", "min", "max", "count", "median"],
        new_column_name: str,
    ) -> None:
        """
        Aplica una agregación por grupo y devuelve el valor agregado alineado a cada fila.
        """
        groups = self._as_column_list(group_columns)
        self._validate_columns(groups + [target_column])

        self.df[new_column_name] = self.df.groupby(groups)[target_column].transform(operation)

    def ranking_por_grupo(
        self,
        group_columns: Union[str, List[str]],
        target_column: str,
        new_column_name: str,
        ascending: bool = False,
        method: Literal["average", "min", "max", "first", "dense"] = "dense",
    ) -> None:
        """
        Calcula ranking dentro de cada grupo.
        """
        groups = self._as_column_list(group_columns)
        self._validate_columns(groups + [target_column])

        self.df[new_column_name] = self.df.groupby(groups)[target_column].rank(ascending=ascending, method=method)

    def acumulado_por_grupo(
        self,
        group_columns: Union[str, List[str]],
        target_column: str,
        new_column_name: str,
    ) -> None:
        """
        Calcula suma acumulada por grupo (orden actual del DataFrame).
        """
        groups = self._as_column_list(group_columns)
        self._validate_columns(groups + [target_column])

        self.df[new_column_name] = self.df.groupby(groups)[target_column].cumsum()

    def promedio_movil(self, column: str, window: int, new_column_name: str, min_periods: Optional[int] = 1) -> None:
        """
        Calcula promedio móvil sobre una columna numérica.
        """
        self._validate_column(column)
        if window <= 0:
            raise ValueError("'window' debe ser mayor que 0.")
        self.df[new_column_name] = self.df[column].rolling(window=window, min_periods=min_periods).mean()

    def suma_movil(self, column: str, window: int, new_column_name: str, min_periods: Optional[int] = 1) -> None:
        """
        Calcula suma móvil sobre una columna numérica.
        """
        self._validate_column(column)
        if window <= 0:
            raise ValueError("'window' debe ser mayor que 0.")
        self.df[new_column_name] = self.df[column].rolling(window=window, min_periods=min_periods).sum()
    
    def buscarh_multiple(self, lookup_row: int, return_rows: List[int], new_column_prefix: str, lookup_value_column: Optional[str] = None, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Variante de BUSCARH que permite devolver múltiples filas en lugar de solo una.
        Si hay múltiples coincidencias en `lookup_row`, devuelve la primera de izquierda a derecha.
        
        Parámetros:
        - lookup_row: La fila en la que se buscará el valor.
        - return_rows: Lista de filas de las cuales se devolverán los valores.
        - new_column_prefix: Prefijo para las nuevas columnas que se agregarán.
        - lookup_value_column: (Opcional) Nombre de la columna que contiene los valores a buscar.
        - not_found_value: Valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        - external_df: (Opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df

        if lookup_row not in df_to_use.index:
            raise ValueError(f"La fila '{lookup_row}' no existe en el DataFrame.")
        
        for row in return_rows:
            if row not in df_to_use.index:
                raise ValueError(f"La fila '{row}' no existe en el DataFrame.")

        lookup_series = df_to_use.loc[lookup_row]

        def get_match_column(value):
            if pd.isna(value):
                matches = lookup_series[lookup_series.isna()]
            else:
                matches = lookup_series[lookup_series == value]
            if matches.empty:
                return None
            return matches.index[0]

        if lookup_value_column is not None:
            if lookup_value_column not in self.df.columns:
                raise ValueError(f"La columna '{lookup_value_column}' no existe en el DataFrame.")
            lookup_values = self.df[lookup_value_column]
        else:
            lookup_values = self._infer_lookup_values_for_row(lookup_row)

        matched_columns = lookup_values.map(get_match_column)

        for row in return_rows:
            self.df[f"{new_column_prefix}_{row}"] = matched_columns.map(
                lambda column_name: df_to_use.at[row, column_name] if column_name is not None else not_found_value
            )

    def buscarv_multiple_conditions(self, conditions: List[Tuple[str, Union[str, int, float]]], return_column: str, new_column_name: str, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Aplica la función BUSCARV con múltiples condiciones a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        conditions: Lista de tuplas que contienen (columna, valor) para las condiciones de búsqueda.
        return_column: La columna de la cual se devolverá el valor.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        not_found_value: El valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df
        
        for col, _ in conditions:
            if col not in df_to_use.columns:
                raise ValueError(f"La columna '{col}' no existe en el DataFrame.")
        if return_column not in df_to_use.columns:
            raise ValueError(f"La columna '{return_column}' no existe en el DataFrame.")

        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")

        mask = pd.Series(True, index=df_to_use.index)
        for col, value in conditions:
            if pd.isna(value):
                mask &= df_to_use[col].isna()
            else:
                mask &= df_to_use[col].eq(value)

        key_column = conditions[0][0]
        if key_column not in self.df.columns:
            raise ValueError(f"La columna '{key_column}' no existe en el DataFrame de trabajo.")
        lookup_series = (
            df_to_use.loc[mask, [key_column, return_column]]
            .drop_duplicates(subset=key_column, keep="first")
            .set_index(key_column)[return_column]
        )
        lookup_dict = lookup_series.to_dict()

        self.df[new_column_name] = self.df[key_column].map(lambda x: lookup_dict.get(x, not_found_value))

    def buscarh_multiple_conditions(self, conditions: List[Tuple[int, Union[str, int, float]]], return_row: int, new_column_name: str, not_found_value: Union[str, int, float] = np.nan, external_df: Optional[pd.DataFrame] = None) -> None:
        """
        Aplica la función BUSCARH con múltiples condiciones a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parameters:
        conditions: Lista de tuplas que contienen (fila, valor) para las condiciones de búsqueda.
        return_row: La fila de la cual se devolverá el valor.
        new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        not_found_value: El valor que se devolverá si no se encuentra el valor buscado (por defecto es NaN).
        external_df: (opcional) DataFrame externo en el que se realizará la búsqueda.
        """
        df_to_use = external_df if external_df is not None else self.df
        
        for row, _ in conditions:
            if row not in df_to_use.index:
                raise ValueError(f"La fila '{row}' no existe en el DataFrame.")
        if return_row not in df_to_use.index:
            raise ValueError(f"La fila '{return_row}' no existe en el DataFrame.")

        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")

        column_mask = pd.Series(True, index=df_to_use.columns)
        for row, value in conditions:
            row_values = df_to_use.loc[row]
            if pd.isna(value):
                column_mask &= row_values.isna()
            else:
                column_mask &= row_values.eq(value)

        matching_columns = df_to_use.columns[column_mask]
        lookup_series = df_to_use.loc[return_row, matching_columns]
        lookup_dict = lookup_series.to_dict()

        self.df[new_column_name] = self.df.index.to_series().map(lambda idx: lookup_dict.get(idx, not_found_value))

    # Devuelve el valor o un valor por defecto si hay error (SI.ERROR)
    def si_error(self, column: str, new_column_name: str, default_value: Union[str, int, float] = np.nan) -> None:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        def to_default_if_error(x):
            if isinstance(x, Exception):
                return default_value
            if isinstance(x, (float, np.floating)) and np.isinf(x):
                return default_value
            return x

        self.df[new_column_name] = self.df[column].apply(to_default_if_error)

    # Cuenta valores numéricos (CONTAR)
    def contar(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = pd.to_numeric(self.df[column], errors='coerce').notnull().astype(int)

    # Cuenta celdas no vacías (CONTARA)
    def contara(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].notnull().astype(int)

    # Devuelve la moda (MODA)
    def moda(self, column: str, new_column_name: Optional[str] = None):
        self._validate_column(column)
        result = self.df[column].mode()[0] if not self.df[column].mode().empty else np.nan
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Varianza poblacional (VAR.P)
    def var_p(self, column: str, new_column_name: Optional[str] = None):
        self._validate_column(column)
        result = self.df[column].var(ddof=0)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Varianza muestral (VAR.S)
    def var_s(self, column: str, new_column_name: Optional[str] = None):
        self._validate_column(column)
        result = self.df[column].var(ddof=1)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Desviación estándar poblacional (DESVEST.P)
    def desv_p(self, column: str, new_column_name: Optional[str] = None):
        self._validate_column(column)
        result = self.df[column].std(ddof=0)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Desviación estándar muestral (DESVEST.S)
    def desv_s(self, column: str, new_column_name: Optional[str] = None):
        self._validate_column(column)
        result = self.df[column].std(ddof=1)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Percentil (PERCENTIL)
    def percentil(self, column: str, q: float, new_column_name: Optional[str] = None):
        self._validate_column(column)
        if not 0 <= q <= 1:
            raise ValueError("'q' debe estar entre 0 y 1.")
        result = self.df[column].quantile(q)
        if new_column_name:
            self.df[new_column_name] = result
        return result

    # Rango (RANK)
    def rango(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].rank()

    # Texto en mayúsculas (MAYUSC)
    def mayusc(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].str.upper()

    # Texto en minúsculas (MINUSC)
    def minusc(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].str.lower()

    # Sustituir texto (SUSTITUIR)
    def sustituir(self, column: str, old: str, new: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].str.replace(old, new, regex=False)

    def sustiuir(self, column: str, old: str, new: str, new_column_name: str) -> None:
        self.sustituir(column, old, new, new_column_name)

    # Formatear valores como texto (TEXTO)
    def texto(self, column: str, format_string: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].map(lambda x: format_string.format(x) if pd.notnull(x) else x)

    # Diferencia de días entre fechas (DIAS)
    def dias(self, start_column: str, end_column: str, new_column_name: str) -> None:
        self._validate_columns([start_column, end_column])
        start_series = self._to_datetime_series(start_column)
        end_series = self._to_datetime_series(end_column)
        self.df[new_column_name] = (end_series - start_series).dt.days

    # Diferencia de horas entre fechas (HORAS)
    def horas(self, start_column: str, end_column: str, new_column_name: str) -> None:
        self._validate_columns([start_column, end_column])
        start_series = self._to_datetime_series(start_column)
        end_series = self._to_datetime_series(end_column)
        self.df[new_column_name] = (end_series - start_series).dt.total_seconds() / 3600

    # Dia de la semana (DIASEM)
    def diasem(self, column: str, new_column_name: str) -> None:
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.dayofweek + 1
    
    # Mes del año (MES)
    def mes(self, column: str, new_column_name: str) -> None:
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.month

    # Año (AÑO)
    def ano(self, column: str, new_column_name: str) -> None:
        datetime_series = self._to_datetime_series(column)
        self.df[new_column_name] = datetime_series.dt.year


    # Número aleatorio entre 0 y 1 (ALEATORIO)
    def aleatorio(self, new_column_name: str) -> None:
        self.df[new_column_name] = np.random.rand(len(self.df))

    # Número entero aleatorio entre dos valores (ALEATORIO.ENTRE)
    def aleatorio_entre(self, new_column_name: str, low: int, high: int) -> None:
        if low > high:
            raise ValueError("'low' no puede ser mayor que 'high'.")
        self.df[new_column_name] = np.random.randint(low, high + 1, size=len(self.df))

    # Verifica si hay error (ESERROR)
    def eserror(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].apply(lambda x: isinstance(x, Exception))

    # Verifica si es número (ESNUMERO)
    def esnumero(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = pd.to_numeric(self.df[column], errors='coerce').notnull()

    # Verifica si está en blanco (ESBLANCO)
    def esblanco(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].isnull()

    # Verifica si es valor lógico (ESLOGICO)
    def eslogico(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].apply(lambda x: isinstance(x, bool))

    # Concatenar múltiples columnas (CONCATENAR)
    def concatenar(self, columns: List[str], new_column_name: str, separator: str = "") -> None:
        self._validate_columns(columns)
        self.df[new_column_name] = self.df[columns].astype(str).agg(separator.join, axis=1)

    # Longitud de texto (LARGO)
    def longitud(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.len()

    # Buscar subcadena en texto (BUSCAR)
    def buscar(self, column: str, substring: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.find(substring)

    # Extraer subcadena (EXTRAER)
    def extraer(self, column: str, start: int, length: int, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str[start:start + length]

    # Truncar número (TRUNCAR)
    def truncar(self, column: str, decimals: int, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].apply(lambda x: np.floor(x * 10**decimals) / 10**decimals if pd.notnull(x) else x)

    # Redondear número (REDONDEAR)
    def redondear_basico(self, column: str, decimals: int, new_column_name: str) -> None:
        self.redondear(column, decimals, new_column_name)

    # Residuo (RESIDUO)
    def residuo(self, column: str, divisor: int, new_column_name: str) -> None:
        self._validate_column(column)
        if divisor == 0:
            raise ValueError("'divisor' no puede ser 0.")
        self.df[new_column_name] = self.df[column] % divisor

    # Potencia (POTENCIA)
    def potencia(self, column: str, exponent: int, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column] ** exponent

    # Valor absoluto (ABS)
    def absoluto(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].abs()

    # Raíz cuadrada (RAIZ)
    def raiz(self, column: str, new_column_name: str) -> None:
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].apply(lambda x: np.sqrt(x) if pd.notnull(x) else x)

from __future__ import annotations

from typing import Callable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd


class CaitSith:
    """Version reducida para pruebas y demo de la app interactiva."""

    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df debe ser un pandas.DataFrame")
        self.df = df.copy()

    def _validate_column(self, column: str) -> None:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

    def _validate_columns(self, columns: List[str]) -> None:
        for column in columns:
            self._validate_column(column)

    def _evaluate_condition(self, column: str, operator: str, value: Union[str, int, float]) -> pd.Series:
        self._validate_column(column)
        series = self.df[column]
        if operator == "==":
            return series == value
        if operator == "!=":
            return series != value
        if operator == ">":
            return series > value
        if operator == ">=":
            return series >= value
        if operator == "<":
            return series < value
        if operator == "<=":
            return series <= value
        raise ValueError(f"Operador no soportado: {operator}")

    def buscarv(
        self,
        lookup_column: str,
        return_column: str,
        new_column_name: str,
        lookup_value_column: Optional[str] = None,
        not_found_value: Union[str, int, float] = np.nan,
        external_df: Optional[pd.DataFrame] = None,
    ) -> None:
        """Busca un valor por columna clave y escribe una nueva columna."""
        df_to_use = external_df if external_df is not None else self.df
        if lookup_column not in df_to_use.columns or return_column not in df_to_use.columns:
            raise ValueError("Las columnas de busqueda no existen en el DataFrame externo.")
        if lookup_value_column is None:
            lookup_value_column = lookup_column
        self._validate_column(lookup_value_column)

        lookup_map = (
            df_to_use[[lookup_column, return_column]]
            .drop_duplicates(subset=lookup_column, keep="first")
            .set_index(lookup_column)[return_column]
            .to_dict()
        )
        self.df[new_column_name] = self.df[lookup_value_column].map(lambda value: lookup_map.get(value, not_found_value))

    def sumar_si(
        self,
        condition_column: str,
        condition_value: Union[str, int, float],
        sum_columns: Union[str, List[str]],
        new_column_name: str,
    ) -> None:
        """Suma columnas por fila solo cuando la condicion se cumple."""
        self._validate_column(condition_column)
        columns = [sum_columns] if isinstance(sum_columns, str) else list(sum_columns)
        self._validate_columns(columns)
        row_sum = self.df[columns].sum(axis=1)
        self.df[new_column_name] = row_sum.where(self.df[condition_column] == condition_value, np.nan)

    def si(
        self,
        conditions: List[Tuple[str, str, Union[str, int, float]]],
        true_value: Union[str, int, float, Callable],
        false_value: Union[str, int, float, Callable],
        new_column_name: str,
        all_conditions: bool = True,
    ) -> None:
        """Aplica una logica IF sobre una o varias condiciones."""
        if not conditions:
            raise ValueError("Debes indicar al menos una condicion.")

        masks = [self._evaluate_condition(column, operator, value) for column, operator, value in conditions]
        final_mask = np.logical_and.reduce(masks) if all_conditions else np.logical_or.reduce(masks)
        true_resolved = self.df.apply(true_value, axis=1) if callable(true_value) else true_value
        false_resolved = self.df.apply(false_value, axis=1) if callable(false_value) else false_value
        self.df[new_column_name] = np.where(final_mask, true_resolved, false_resolved)

    def filtrar(
        self,
        conditions: List[Tuple[str, str, Union[str, int, float]]],
        logic: str = "and",
    ) -> pd.DataFrame:
        """Devuelve un DataFrame filtrado, sin modificar self.df."""
        if not conditions:
            raise ValueError("Debes indicar al menos una condicion.")
        masks = [self._evaluate_condition(column, operator, value) for column, operator, value in conditions]
        if logic.lower() == "and":
            final_mask = np.logical_and.reduce(masks)
        elif logic.lower() == "or":
            final_mask = np.logical_or.reduce(masks)
        else:
            raise ValueError("logic debe ser 'and' o 'or'.")
        return self.df.loc[final_mask].copy()

    def izquierda(self, column: str, num_chars: int, new_column_name: str) -> None:
        """Extrae los primeros caracteres de una columna de texto."""
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str[:num_chars]

    def reemplazar(self, column: str, old_value: str, new_value: str, new_column_name: str) -> None:
        """Reemplaza texto exacto en una nueva columna."""
        self._validate_column(column)
        self.df[new_column_name] = self.df[column].astype(str).str.replace(old_value, new_value, regex=False)

    def coalesce(self, columns: Union[str, List[str]], new_column_name: str) -> None:
        """Devuelve el primer valor no nulo entre varias columnas."""
        columns_list = [columns] if isinstance(columns, str) else list(columns)
        self._validate_columns(columns_list)
        self.df[new_column_name] = self.df[columns_list].bfill(axis=1).iloc[:, 0]

    def rellenar(self, column: str, fill_value: Union[str, int, float], new_column_name: Optional[str] = None) -> None:
        """Rellena nulos en una columna."""
        self._validate_column(column)
        if new_column_name is None:
            self.df[column] = self.df[column].fillna(fill_value)
        else:
            self.df[new_column_name] = self.df[column].fillna(fill_value)

    def ordenar_por(self, columns: Union[str, List[str]], ascending: bool = True, reset_index: bool = False) -> None:
        """Ordena el DataFrame por una o varias columnas."""
        columns_list = [columns] if isinstance(columns, str) else list(columns)
        self._validate_columns(columns_list)
        self.df = self.df.sort_values(by=columns_list, ascending=ascending)
        if reset_index:
            self.df = self.df.reset_index(drop=True)
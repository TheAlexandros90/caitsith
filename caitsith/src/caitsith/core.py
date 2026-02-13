import numpy as np
import pandas as pd
from typing import Union, List, Tuple, Callable,Optional
import html

class CaitSith:

    """ Alvaro me obliga a poner que esta clase es de él, pero no lo es. Es mía.
     Esta clase permite realizar operaciones similares a las funciones BUSCARV, BUSCARH, SUMAR.SI y RESTAR.SI de Excel en un DataFrame de pandas."""
    def __init__(self, df: pd.DataFrame):
        if isinstance(df, pd.DataFrame):
            self.df = df
        else:
            raise ValueError("El input debe ser un DataFrame de pandas")

    def buscarv(self, lookup_column: str, return_column: str, new_column_name: str, lookup_value_column: str = None, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
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
        
        lookup_series = df_to_use.set_index(lookup_column)[return_column]
        
        if lookup_value_column:
            self.df[new_column_name] = self.df[lookup_value_column].apply(lambda x: lookup_series.get(x, not_found_value))
        else:
            self.df[new_column_name] = self.df[lookup_column].apply(lambda x: lookup_series.get(x, not_found_value))
    
    def buscarh(self, lookup_row: int, return_row: int, new_column_name: str, lookup_value_column: str = None, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
        """
        Aplica la función BUSCARH a todas las filas del DataFrame y agrega una nueva columna con los resultados.
        
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
        if lookup_value_column:
            self.df[new_column_name] = self.df[lookup_value_column].apply(lambda x: lookup_series[lookup_series == x].index[0] if x in lookup_series.values else not_found_value)
        else:
            self.df[new_column_name] = self.df.apply(lambda row: lookup_series[lookup_series == row[lookup_row]].index[0] if row[lookup_row] in lookup_series.values else not_found_value, axis=1)

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
        
        condition_met = self.df[condition_column] == condition_value
        self.df[new_column_name] = self.df.loc[condition_met, sum_columns].sum(axis=1)
        
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
        
        condition_met = self.df[condition_column] == condition_value
        self.df[new_column_name] = self.df.loc[condition_met, subtract_columns].apply(lambda row: row.iloc[0] - row.iloc[1] if len(row) > 1 else row.iloc[0], axis=1)

    def indice_coincidir(self, lookup_column: str, return_column: str, new_column_name: str, lookup_value_column: str = None, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
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
        
        def get_match(x):
            match_idx = df_to_use[lookup_column].eq(x).idxmax()
            if match_idx is None or pd.isna(match_idx):
                return not_found_value
            return df_to_use.at[match_idx, return_column]
        
        if lookup_value_column:
            self.df[new_column_name] = self.df[lookup_value_column].apply(get_match)
        else:
            self.df[new_column_name] = self.df[lookup_column].apply(get_match)

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

    def dividir(self, column: str, split_char: str, new_column_prefix: str, keep_split_char: bool = False, split_all: bool = True) -> None:
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
        Devuelve la posición de la primera aparición de una subcadena dentro de la cadena en la columna `   column`, similar a `encontrar`.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[column].str.find(find_string)

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

    def si(self, conditions: List[Tuple[str, str, Union[str, int, float]]], true_value: Union[str, int, float, Callable], false_value: Union[str, int, float, Callable], new_column_name: str, all_conditions: bool = True) -> None:
        """
        Aplica una lógica condicional tipo SI (IF) a las filas del DataFrame y agrega una nueva columna con los resultados.
        
        Parámetros:
        - conditions: Lista de tuplas que contienen (columna, operador, valor). Ejemplo: [("columna1", "==", 10), ("columna2", ">", 5)]
        - true_value: El valor o función que se devolverá si la condición es verdadera.
        - false_value: El valor o función que se devolverá si la condición es falsa.
        - new_column_name: El nombre de la nueva columna que se agregará al DataFrame.
        - all_conditions: Si es True, todas las condiciones deben cumplirse. Si es False, al menos una condición debe cumplirse.
        """
        if not conditions:
            raise ValueError("La lista de condiciones no puede estar vacía.")
        
        condition_results = []
        
        for condition_column, operator, value in conditions:
            if condition_column not in self.df.columns:
                raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
            
            if operator == "==":
                condition_met = self.df[condition_column] == value
            elif operator == "!=":
                condition_met = self.df[condition_column] != value
            elif operator == ">":
                condition_met = self.df[condition_column] > value
            elif operator == "<":
                condition_met = self.df[condition_column] < value
            elif operator == ">=":
                condition_met = self.df[condition_column] >= value
            elif operator == "<=":
                condition_met = self.df[condition_column] <= value
            else:
                raise ValueError(f"Operador '{operator}' no soportado.")
            
            condition_results.append(condition_met)
        
        if all_conditions:
            final_condition = np.logical_and.reduce(condition_results)
        else:
            final_condition = np.logical_or.reduce(condition_results)
        
        if callable(true_value):
            self.df[new_column_name] = np.where(final_condition, self.df.apply(lambda row: true_value(row), axis=1), false_value)
        elif callable(false_value):
            self.df[new_column_name] = np.where(final_condition, true_value, self.df.apply(lambda row: false_value(row), axis=1))
        else:
            self.df[new_column_name] = np.where(final_condition, true_value, false_value)

    def contar_si(self, condition_column: str, condition_value: Union[str, int, float], new_column_name: str) -> None:
        """
        Cuenta el número de veces que `condition_value` aparece en la columna `condition_column` y coloca el resultado en `new_column_name`.
        """
        if condition_column not in self.df.columns:
            raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[condition_column].apply(lambda x: x.count(condition_value))  

    def contar_si_conjunto(self, condition_columns: List[str], condition_values: List[Union[str, int, float]], new_column_name: str) -> None:
        """
        Cuenta el número de veces que se cumple un conjunto de condiciones en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        if len(condition_columns) != len(condition_values):
            raise ValueError("Las listas de columnas y valores de condición deben tener la misma longitud.")
        
        condition_met = self.df[condition_columns].eq(condition_values).all(axis=1)
        self.df[new_column_name] = condition_met.sum()

    def contar_si_rango(self, condition_column: str, lower_bound: Union[int, float], upper_bound: Union[int, float], new_column_name: str) -> None:
        """
        Cuenta el número de veces que los valores en `condition_column` están dentro del rango [lower_bound, upper_bound] y coloca el resultado en `new_column_name`.
        """
        if condition_column not in self.df.columns:
            raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
        self.df[new_column_name] = self.df[condition_column].apply(lambda x: (lower_bound <= x <= upper_bound))

    def contar_si_conjunto_rango(self, condition_columns: List[str], lower_bounds: List[Union[int, float]], upper_bounds: List[Union[int, float]], new_column_name: str) -> None:
        """
        Cuenta el número de veces que se cumple un conjunto de condiciones de rango en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        if len(condition_columns) != len(lower_bounds) or len(condition_columns) != len(upper_bounds):
            raise ValueError("Las listas de columnas y valores de condición deben tener la misma longitud.")
        
        condition_met = self.df[condition_columns].apply(lambda row: all(lower <= x <= upper for x, lower, upper in zip(row, lower_bounds, upper_bounds)), axis=1)
        self.df[new_column_name] = condition_met.sum()

    def sumaproducto(self, columns: List[str], new_column_name: str) -> None:
        """
        Calcula el producto de los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].product(axis=1)

    def sumar(self, columns: List[str], new_column_name: str) -> None:
        """
        Suma los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].sum(axis=1)

    def restar(self, columns: List[str], new_column_name: str) -> None:
        """
        Resta los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].apply(lambda row: row.iloc[0] - row.iloc[1] if len(row) > 1 else row.iloc[0], axis=1)

    def multiplicar(self, columns: List[str], new_column_name: str) -> None:
        """
        Multiplica los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].product(axis=1)

    def dividir(self, columns: List[str], new_column_name: str) -> None:
        """
        Divide los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].apply(lambda row: row.iloc[0] / row.iloc[1] if row.iloc[1] != 0 else np.nan, axis=1)

    def promedio(self, columns: List[str], new_column_name: str) -> None:
        """
        Calcula el promedio de los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].mean(axis=1)

    def mediana(self, columns: List[str], new_column_name: str) -> None:
        """
        Calcula la mediana de los valores en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
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
    
        condition_results = []
        for condition_column, operator, value in conditions:
            if condition_column not in self.df.columns:
                raise ValueError(f"La columna '{condition_column}' no existe en el DataFrame.")
            if operator == "==":
                condition_met = self.df[condition_column] == value
            elif operator == "!=":
                condition_met = self.df[condition_column] != value
            elif operator == ">":
                condition_met = self.df[condition_column] > value
            elif operator == "<":
                condition_met = self.df[condition_column] < value
            elif operator == ">=":
                condition_met = self.df[condition_column] >= value
            elif operator == "<=":
                condition_met = self.df[condition_column] <= value
            else:
                raise ValueError(f"Operador '{operator}' no soportado.")
            condition_results.append(condition_met)
    
        if logic.lower() == 'and':
            self.df[new_column_name] = np.logical_and.reduce(condition_results)
        elif logic.lower() == 'or':
            self.df[new_column_name] = np.logical_or.reduce(condition_results)
        else:
            raise ValueError("El valor de 'logic' debe ser 'and' o 'or'.")
        
    def contar_valores(self, columns: List[str], new_column_name: str) -> None:
        """
        Cuenta el número de valores únicos en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].nunique(axis=1)

    def contar_valores_unicos(self, columns: List[str], new_column_name: str) -> None:
        """
        Cuenta el número de valores únicos en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].nunique(axis=1)

    def contar_valores_duplicados(self, columns: List[str], new_column_name: str) -> None:
        """
        Cuenta el número de valores duplicados en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].duplicated(keep=False).sum()

    def contar_valores_unicos_duplicados(self, columns: List[str], new_column_name: str) -> None:
        """
        Cuenta el número de valores únicos duplicados en las columnas especificadas y coloca el resultado en `new_column_name`.
        """
        self.df[new_column_name] = self.df[columns].duplicated(keep='first').sum()

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
        
        hyperlink_series = self.df.apply(
            lambda row: f'<a href="{html.escape(row[url_column])}">{html.escape(row[display_text_column])}</a>', 
            axis=1
        )
        
        if new_column_name:
            self.df[new_column_name] = hyperlink_series
        else:
            self.df[url_column] = hyperlink_series

    def buscarv_multiple(self, lookup_column: str, return_columns: List[str], new_column_prefix: str, lookup_value_column: str = None, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
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
        
        for col in return_columns:
            if col not in df_to_use.columns:
                raise ValueError(f"La columna '{col}' no existe en el DataFrame.")

        lookup_series = df_to_use.set_index(lookup_column)[return_columns]

        if lookup_value_column:
            for col in return_columns:
                self.df[f"{new_column_prefix}_{col}"] = self.df[lookup_value_column].apply(lambda x: lookup_series[col].get(x, not_found_value))
        else:
            for col in return_columns:
                self.df[f"{new_column_prefix}_{col}"] = self.df[lookup_column].apply(lambda x: lookup_series[col].get(x, not_found_value))

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
        
        self.df[new_column_name] = self.df[column].str.extract('(\d+)')

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

        regex_pattern = f"[{characters}]"
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
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        if new_column_name:
            self.df[new_column_name] = self.df[column].dt.strftime(format_string)
        else:
            self.df[column] = self.df[column].dt.strftime(format_string)

    def texto_a_fecha(self, column: str, format_string: str = "%d/%m/%Y", new_column_name: Optional[str] = None) -> None:
        """
        Convierte una columna de texto en fechas con un formato específico.
        
        Parámetros:
        - column: Nombre de la columna con valores de texto.
        - format_string: Formato en que se representará la fecha (por defecto 'DD/MM/YYYY').
        - new_column_name: (Opcional) Si se proporciona, los resultados se almacenarán en una nueva columna.
        """
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")

        if new_column_name:
            self.df[new_column_name] = pd.to_datetime(self.df[column], format=format_string, errors='coerce')
        else:
            self.df[column] = pd.to_datetime(self.df[column], format=format_string, errors='coerce')

    def diferencia_fechas(self, start_column: str, end_column: str, unit: str = "days", new_column_name: str = "Diferencia") -> None:
        """
        Calcula la diferencia entre dos columnas de fechas en la unidad especificada (días, horas, minutos, segundos).
        
        Parámetros:
        - start_column: Nombre de la columna con la fecha de inicio.
        - end_column: Nombre de la columna con la fecha final.
        - unit: Unidad de medida para la diferencia ('days', 'hours', 'minutes', 'seconds').
        - new_column_name: Nombre de la nueva columna que almacenará la diferencia de fechas.
        """
        if start_column not in self.df.columns or end_column not in self.df.columns:
            raise ValueError("Una o ambas columnas de fecha no existen en el DataFrame.")
        
        delta = self.df[end_column] - self.df[start_column]

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
    
    def buscarh_multiple(self, lookup_row: int, return_rows: List[int], new_column_prefix: str, lookup_value_column: str = None, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
        """
        Variante de BUSCARH que permite devolver múltiples filas en lugar de solo una.
        
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

        if lookup_value_column:
            for row in return_rows:
                self.df[f"{new_column_prefix}_{row}"] = self.df[lookup_value_column].apply(lambda x: df_to_use.loc[row, lookup_series[lookup_series == x].index[0]] if x in lookup_series.values else not_found_value)
        else:
            for row in return_rows:
                self.df[f"{new_column_prefix}_{row}"] = self.df.apply(lambda row_data: df_to_use.loc[row, lookup_series[lookup_series == row_data[lookup_row]].index[0]] if row_data[lookup_row] in lookup_series.values else not_found_value, axis=1)

    def buscarv_multiple_conditions(self, conditions: List[Tuple[str, Union[str, int, float]]], return_column: str, new_column_name: str, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
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
        
        def match_conditions(row):
            for col, value in conditions:
                if row[col] != value:
                    return False
            return True
        
        lookup_series = df_to_use[df_to_use.apply(match_conditions, axis=1)].set_index(conditions[0][0])[return_column]
        
        self.df[new_column_name] = self.df.apply(lambda row: lookup_series.get(row[conditions[0][0]], not_found_value), axis=1)

    def buscarh_multiple_conditions(self, conditions: List[Tuple[int, Union[str, int, float]]], return_row: int, new_column_name: str, not_found_value: Union[str, int, float] = np.nan, external_df: pd.DataFrame = None) -> None:
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
        
        def match_conditions(column):
            for row, value in conditions:
                if df_to_use.at[row, column] != value:
                    return False
            return True
        
        lookup_series = df_to_use.loc[:, df_to_use.apply(match_conditions, axis=0)].loc[return_row]
        
        self.df[new_column_name] = self.df.apply(lambda row: lookup_series.get(row.name, not_found_value), axis=1)

    # Devuelve el valor o un valor por defecto si hay error (SI.ERROR)
    def si_error(self, column: str, new_column_name: str, default_value: Union[str, int, float] = np.nan) -> None:
        if column not in self.df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
        def try_or_default(x):
            try:
                return x
            except Exception:
                return default_value
        self.df[new_column_name] = self.df[column].apply(lambda x: try_or_default(x))

    # Cuenta valores numéricos (CONTAR)
    def contar(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = pd.to_numeric(self.df[column], errors='coerce').notnull().astype(int)

    # Cuenta celdas no vacías (CONTARA)
    def contara(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].notnull().astype(int)

    # Devuelve la moda (MODA)
    def moda(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].mode()[0] if not self.df[column].mode().empty else np.nan

    # Varianza poblacional (VAR.P)
    def var_p(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].var(ddof=0)

    # Varianza muestral (VAR.S)
    def var_s(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].var(ddof=1)

    # Desviación estándar poblacional (DESVEST.P)
    def desv_p(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].std(ddof=0)

    # Desviación estándar muestral (DESVEST.S)
    def desv_s(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].std(ddof=1)

    # Percentil (PERCENTIL)
    def percentil(self, column: str, q: float, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].quantile(q)

    # Rango (RANK)
    def rango(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].rank()

    # Texto en mayúsculas (MAYUSC)
    def mayusc(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].str.upper()

    # Texto en minúsculas (MINUSC)
    def minusc(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].str.lower()

    # Sustituir texto (SUSTITUIR)
    def sustiuir(self, column: str, old: str, new: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].str.replace(old, new, regex=False)

    # Formatear valores como texto (TEXTO)
    def texto(self, column: str, format_string: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].map(lambda x: format_string.format(x) if pd.notnull(x) else x)

    # Diferencia de días entre fechas (DIAS)
    def dias(self, start_column: str, end_column: str, new_column_name: str) -> None:
        self.df[new_column_name] = (self.df[end_column] - self.df[start_column]).dt.days


    # Número aleatorio entre 0 y 1 (ALEATORIO)
    def aleatorio(self, new_column_name: str) -> None:
        self.df[new_column_name] = np.random.rand(len(self.df))

    # Número entero aleatorio entre dos valores (ALEATORIO.ENTRE)
    def aleatorio_entre(self, new_column_name: str, low: int, high: int) -> None:
        self.df[new_column_name] = np.random.randint(low, high + 1, size=len(self.df))

    # Verifica si hay error (ESERROR)
    def eserror(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].apply(lambda x: isinstance(x, Exception))

    # Verifica si es número (ESNUMERO)
    def esnumero(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = pd.to_numeric(self.df[column], errors='coerce').notnull()

    # Verifica si está en blanco (ESBLANCO)
    def esblanco(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].isnull()

    # Verifica si es valor lógico (ESLOGICO)
    def eslogico(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].apply(lambda x: isinstance(x, bool))

    # Concatenar múltiples columnas (CONCATENAR)
    def concatenar(self, columns: list, new_column_name: str, separator: str = "") -> None:
        self.df[new_column_name] = self.df[columns].astype(str).agg(separator.join, axis=1)

    # Longitud de texto (LARGO)
    def longitud(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].astype(str).str.len()

    # Buscar subcadena en texto (BUSCAR)
    def buscar(self, column: str, substring: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].astype(str).str.find(substring)

    # Extraer subcadena (EXTRAER)
    def extraer(self, column: str, start: int, length: int, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].astype(str).str[start:start + length]

    # Truncar número (TRUNCAR)
    def truncar(self, column: str, decimals: int, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].apply(lambda x: np.floor(x * 10**decimals) / 10**decimals if pd.notnull(x) else x)

    # Redondear número (REDONDEAR)
    def redondear(self, column: str, decimals: int, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].round(decimals)

    # Residuo (RESIDUO)
    def residuo(self, column: str, divisor: int, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column] % divisor

    # Potencia (POTENCIA)
    def potencia(self, column: str, exponent: int, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column] ** exponent

    # Valor absoluto (ABS)
    def absoluto(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].abs()

    # Raíz cuadrada (RAIZ)
    def raiz(self, column: str, new_column_name: str) -> None:
        self.df[new_column_name] = self.df[column].apply(lambda x: np.sqrt(x) if pd.notnull(x) else x)

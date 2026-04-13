# CaitSith

CaitSith es una libreria de utilidades tipo Excel para pandas DataFrames.

Incluye operaciones de busqueda, transformacion de texto, fechas, logica condicional,
agregaciones, funciones financieras y utilidades de limpieza sobre DataFrames.

## Instalacion

Instalacion base:

```bash
pip install -e .
```

Instalacion con la app interactiva CaitSith Studio:

```bash
pip install -e ".[app]"
```

Instalacion para notebooks:

```bash
pip install -e ".[notebook]"
```

## Uso rapido

```python
import pandas as pd

from caitsith import CaitSith

df = pd.DataFrame(
	{
		"cliente": ["A", "B", "C"],
		"importe": [100, 150, 120],
		"coste": [60, 80, 90],
	}
)

cs = CaitSith(df)
cs.restar(["importe", "coste"], "margen")

print(cs.df)
```

## CaitSith Studio

Este repositorio incluye una app Streamlit para construir pipelines visuales sobre la clase `CaitSith`.

Caracteristicas principales:

- Carga el `CaitSith` real del repo por defecto.
- Permite cargar un core externo desde `.py` o `.ipynb`.
- Detecta metodos, firmas y docstrings automaticamente.
- Construye pipelines por pasos con preview, ejecucion parcial y rollback.
- Exporta el pipeline a JSON o YAML.
- Genera el codigo Python equivalente.

Para lanzarla:

```bash
streamlit run app.py
```

## Estructura

```text
caitsith/
|-- app.py
|-- pyproject.toml
|-- README.md
`-- src/
	|-- caitsith/
	`-- caitsith_studio/
```

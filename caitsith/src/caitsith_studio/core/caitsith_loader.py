from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class LoadedCaitSith:
    caitsith_class: type
    source_name: str
    source_text: str


def load_caitsith_from_path(path: str | Path) -> LoadedCaitSith:
    resolved = Path(path).expanduser().resolve()
    source_text = _read_source_text(resolved.read_text(encoding="utf-8"), resolved.suffix.lower())
    return _compile_caitsith(source_text, source_name=str(resolved))


def load_caitsith_from_bytes(filename: str, content: bytes) -> LoadedCaitSith:
    suffix = Path(filename).suffix.lower()
    raw_text = content.decode("utf-8")
    source_text = _read_source_text(raw_text, suffix)
    return _compile_caitsith(source_text, source_name=filename)


def _read_source_text(raw_text: str, suffix: str) -> str:
    if suffix == ".ipynb":
        payload = json.loads(raw_text)
        code_cells = [cell for cell in payload.get("cells", []) if cell.get("cell_type") == "code"]
        return "\n\n".join("".join(cell.get("source", [])) for cell in code_cells)
    if suffix == ".py":
        return raw_text
    raise ValueError("Solo se soportan fuentes .py o .ipynb para cargar CaitSith.")


def _compile_caitsith(source_text: str, source_name: str) -> LoadedCaitSith:
    module_ast = ast.parse(source_text, filename=source_name)
    filtered_nodes: list[ast.AST] = []
    for node in module_ast.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            filtered_nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "CaitSith":
            filtered_nodes.append(node)

    if not filtered_nodes:
        raise ValueError("No se encontro una clase publica llamada 'CaitSith' en la fuente indicada.")

    filtered_module = ast.Module(body=filtered_nodes, type_ignores=[])
    ast.fix_missing_locations(filtered_module)

    namespace: dict[str, Any] = {"__name__": "caitsith_runtime"}
    compiled = compile(filtered_module, filename=source_name, mode="exec")
    exec(compiled, namespace, namespace)

    caitsith_class = namespace.get("CaitSith")
    if caitsith_class is None:
        raise ValueError("La fuente se compilo, pero no produjo la clase 'CaitSith'.")

    return LoadedCaitSith(
        caitsith_class=caitsith_class,
        source_name=source_name,
        source_text=source_text,
    )
from __future__ import annotations

from dataclasses import asdict
import inspect
import json
from typing import Any

import yaml

from caitsith_studio.models import PipelineStep


def pipeline_to_dict(steps: list[PipelineStep]) -> dict[str, Any]:
    return {
        "version": 1,
        "steps": [_serialize_step(step) for step in steps],
    }


def pipeline_from_dict(payload: dict[str, Any]) -> list[PipelineStep]:
    step_payloads = payload.get("steps", [])
    return [PipelineStep(**step_payload) for step_payload in step_payloads]


def pipeline_to_json(steps: list[PipelineStep]) -> str:
    return json.dumps(pipeline_to_dict(steps), ensure_ascii=False, indent=2)


def pipeline_from_json(raw_text: str) -> list[PipelineStep]:
    return pipeline_from_dict(json.loads(raw_text))


def pipeline_to_yaml(steps: list[PipelineStep]) -> str:
    return yaml.safe_dump(pipeline_to_dict(steps), allow_unicode=True, sort_keys=False)


def pipeline_from_yaml(raw_text: str) -> list[PipelineStep]:
    payload = yaml.safe_load(raw_text)
    if not isinstance(payload, dict):
        raise ValueError("El contenido YAML del pipeline debe ser un objeto/diccionario.")
    return pipeline_from_dict(payload)


def generate_python_code(
    steps: list[PipelineStep],
    *,
    caitsith_import: str = "from caitsith import CaitSith",
    caitsith_class: type | None = None,
) -> str:
    lines: list[str] = [
        "import pandas as pd",
        caitsith_import,
        "",
        "# TODO: carga aqui tus DataFrames reales",
        "frames = {}",
        "",
    ]

    for step in sorted(steps, key=lambda current: (current.step_order, current.id)):
        if not step.enabled:
            lines.append(f"# Paso {step.step_order} desactivado: {step.formula}")
            continue

        lines.append(f"# Paso {step.step_order}: {step.formula} sobre '{step.df_name}'")
        lines.append(f"runtime = CaitSith(frames[{step.df_name!r}].copy())")
        kwargs_repr = []
        for key, value in _iter_valid_parameters(step, caitsith_class):
            if key == "external_df" and isinstance(value, str):
                kwargs_repr.append(f"{key}=frames[{value!r}]")
            else:
                kwargs_repr.append(f"{key}={repr(value)}")
        joined_kwargs = ", ".join(kwargs_repr)
        lines.append(f"result = runtime.{step.formula}({joined_kwargs})")
        lines.append("frames[%r] = result.copy() if isinstance(result, pd.DataFrame) else runtime.df.copy()" % step.df_name)
        lines.append("")

    lines.append("final_df = frames")
    return "\n".join(lines)


def _serialize_step(step: PipelineStep) -> dict[str, Any]:
    serialized = asdict(step)
    serialized["parameters"] = _to_json_safe(serialized["parameters"])
    return serialized


def _iter_valid_parameters(step: PipelineStep, caitsith_class: type | None) -> list[tuple[str, Any]]:
    if caitsith_class is None or not hasattr(caitsith_class, step.formula):
        return list(step.parameters.items())

    method = getattr(caitsith_class, step.formula)
    signature = inspect.signature(method)
    valid_names = {name for name in signature.parameters if name != "self"}
    return [(key, value) for key, value in step.parameters.items() if key in valid_names]


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_json_safe(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_to_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return repr(value)
    return value
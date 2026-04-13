from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd


ParameterWidgetKind = Literal[
    "text",
    "number",
    "bool",
    "column",
    "columns",
    "literal",
    "dataframe",
    "conditions",
    "condition_groups",
    "value_list",
    "condition_pairs",
    "range_pairs",
    "json",
]
OperationKind = Literal[
    "column_transform",
    "row_filter",
    "aggregation",
    "lookup",
    "text_transform",
    "conditional",
    "sort",
    "date_time",
    "finance",
    "groupby",
    "math",
    "utility",
]
OperationReturnKind = Literal["mutates_df", "dataframe", "scalar", "series", "unknown"]
StepStatus = Literal["pending", "executed", "error", "disabled"]


@dataclass(slots=True)
class OperationParameter:
    name: str
    label: str
    annotation: str
    required: bool
    default: Any = None
    widget: ParameterWidgetKind = "text"
    help_text: str = ""
    choices: list[Any] = field(default_factory=list)
    is_output_name: bool = False
    accepts_column_values: bool = False
    editor_columns: list[str] = field(default_factory=list)


@dataclass(slots=True)
class OperationSpec:
    name: str
    display_name: str
    category: str
    kind: OperationKind
    docstring: str
    signature_text: str
    parameters: list[OperationParameter]
    return_kind: OperationReturnKind
    mutates_dataframe: bool
    creates_column: bool
    returns_dataframe: bool


@dataclass(slots=True)
class PipelineStep:
    id: str
    enabled: bool
    step_order: int
    df_name: str
    formula: str
    parameters: dict[str, Any] = field(default_factory=dict)
    status: StepStatus = "pending"
    preview: str = ""
    notes: str = ""
    last_error: str | None = None
    last_warning: str | None = None

    def target_label(self) -> str:
        for key in ("new_column_name", "target_column", "new_column_prefix"):
            value = self.parameters.get(key)
            if value:
                return str(value)
        return ""


@dataclass(slots=True)
class ExecutionLog:
    timestamp: str
    step_id: str
    step_order: int
    df_name: str
    formula: str
    status: StepStatus
    message: str
    warning: str | None = None
    duration_ms: float | None = None

    @classmethod
    def create(
        cls,
        *,
        step_id: str,
        step_order: int,
        df_name: str,
        formula: str,
        status: StepStatus,
        message: str,
        warning: str | None = None,
        duration_ms: float | None = None,
    ) -> "ExecutionLog":
        return cls(
            timestamp=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            step_id=step_id,
            step_order=step_order,
            df_name=df_name,
            formula=formula,
            status=status,
            message=message,
            warning=warning,
            duration_ms=duration_ms,
        )


@dataclass(slots=True)
class PreviewArtifact:
    step_id: str
    df_name: str
    summary: str
    validation_errors: list[str] = field(default_factory=list)
    validation_warnings: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    changed_columns: list[str] = field(default_factory=list)
    row_delta: int = 0
    before_head: pd.DataFrame = field(default_factory=pd.DataFrame)
    after_head: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass(slots=True)
class StepExecutionDetail:
    step_id: str
    df_name: str
    status: StepStatus
    message: str
    warning: str | None
    before_shape: tuple[int, int]
    after_shape: tuple[int, int]
    new_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    changed_columns: list[str] = field(default_factory=list)
    return_value_repr: str | None = None


@dataclass(slots=True)
class PipelineResult:
    frames: dict[str, pd.DataFrame]
    snapshots: dict[str, dict[str, pd.DataFrame]] = field(default_factory=dict)
    logs: list[ExecutionLog] = field(default_factory=list)
    step_details: dict[str, StepExecutionDetail] = field(default_factory=dict)
    generated_code: str = ""


@dataclass(slots=True)
class DataFrameRegistry:
    frames: dict[str, pd.DataFrame] = field(default_factory=dict)
    metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    active_name: str | None = None

    def add(self, name: str, frame: pd.DataFrame, *, source: str = "manual", description: str = "") -> None:
        if not name:
            raise ValueError("El nombre del DataFrame no puede estar vacio.")
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Solo se pueden registrar pandas.DataFrame.")
        self.frames[name] = frame.copy()
        self.metadata[name] = {
            "source": source,
            "description": description,
            "shape": tuple(frame.shape),
            "columns": frame.columns.tolist(),
            "dtypes": {column: str(dtype) for column, dtype in frame.dtypes.items()},
        }
        if self.active_name is None:
            self.active_name = name

    def set_active(self, name: str) -> None:
        if name not in self.frames:
            raise KeyError(f"No existe el DataFrame '{name}'.")
        self.active_name = name

    def get(self, name: str) -> pd.DataFrame:
        if name not in self.frames:
            raise KeyError(f"No existe el DataFrame '{name}'.")
        return self.frames[name]

    def names(self) -> list[str]:
        return sorted(self.frames)

    def copy_frames(self) -> dict[str, pd.DataFrame]:
        return {name: frame.copy() for name, frame in self.frames.items()}

    def info_frame(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for name in self.names():
            frame = self.frames[name]
            rows.append(
                {
                    "df_name": name,
                    "rows": frame.shape[0],
                    "columns": frame.shape[1],
                    "source": self.metadata.get(name, {}).get("source", "manual"),
                    "active": name == self.active_name,
                }
            )
        return pd.DataFrame(rows)
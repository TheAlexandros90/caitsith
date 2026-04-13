from __future__ import annotations

from dataclasses import dataclass, field

from caitsith_studio.core.introspection import build_operation_specs
from caitsith_studio.models import OperationSpec


@dataclass(slots=True)
class OperationRegistry:
    caitsith_class: type
    operations: dict[str, OperationSpec] = field(default_factory=dict)

    @classmethod
    def from_caitsith_class(cls, caitsith_class: type) -> "OperationRegistry":
        specs = build_operation_specs(caitsith_class)
        return cls(caitsith_class=caitsith_class, operations={spec.name: spec for spec in specs})

    def names(self) -> list[str]:
        return sorted(self.operations)

    def specs(self) -> list[OperationSpec]:
        return [self.operations[name] for name in self.names()]

    def get(self, name: str) -> OperationSpec:
        if name not in self.operations:
            raise KeyError(f"La operacion '{name}' no esta registrada.")
        return self.operations[name]

    def by_category(self) -> dict[str, list[OperationSpec]]:
        grouped: dict[str, list[OperationSpec]] = {}
        for spec in self.specs():
            grouped.setdefault(spec.category, []).append(spec)
        return grouped
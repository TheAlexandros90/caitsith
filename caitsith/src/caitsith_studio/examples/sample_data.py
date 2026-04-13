from __future__ import annotations

import pandas as pd


def build_sample_frames() -> dict[str, pd.DataFrame]:
    ventas = pd.DataFrame(
        {
            "cliente_id": [101, 101, 102, 103, 104, 104],
            "fecha": pd.to_datetime([
                "2026-01-03",
                "2026-01-18",
                "2026-02-02",
                "2026-02-12",
                "2026-03-01",
                "2026-03-10",
            ]),
            "importe": [120.0, 85.0, 210.0, 70.0, 130.0, 300.0],
            "coste": [75.0, 40.0, 155.0, 45.0, 80.0, 220.0],
            "canal": ["web", "tienda", "web", "web", "tienda", "web"],
            "comentario": ["A-001", "A-002", "B-009", "B-011", "C-210", "C-211"],
        }
    )

    clientes = pd.DataFrame(
        {
            "cliente_id": [101, 102, 103, 104],
            "segmento": ["vip", "growth", "core", "vip"],
            "pais": ["ES", "ES", "PT", "ES"],
        }
    )

    return {
        "ventas": ventas,
        "clientes": clientes,
    }
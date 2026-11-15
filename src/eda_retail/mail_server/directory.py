"""Directorio sintético de clientes del servidor de correo (supuesto A3 de odd/tasks/v2-mcp-a2a.md).

Una dirección `<customer_ref>@example.test` (dominio reservado, nunca se entrega) para cada cliente que
producen el generador sintético (`CUST-0000` a `CUST-9999`) y los datos de ejemplo de la demo. La dirección
solo vive en `mail_server`: no sale en la salida de la herramienta, ni en los eventos, ni en los logs.
"""

from __future__ import annotations

from eda_retail.synthetic.generator import CUSTOMERS, customer_ref
from eda_retail.synthetic.sample_data import ORDERS


def address_for(ref: str) -> str:
    return f"{ref.lower()}@example.test"


def customer_refs() -> list[str]:
    """Todos los clientes sintéticos y de ejemplo, en un orden fijo y sin repetidos."""
    refs = [customer_ref(n) for n in range(CUSTOMERS)] + [c for _, c, *_ in ORDERS]
    return list(dict.fromkeys(refs))


def synthetic_directory() -> dict[str, str]:
    return {ref: address_for(ref) for ref in customer_refs()}

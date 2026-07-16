#!/usr/bin/env python3
# Lee eventos de Debezium (uno por línea, vía stdin) y muestra solo lo
# relevante para verificar un cambio: tipo de operación, id, y los campos
# antes/después. Genérico para cualquier tabla (no asume columnas fijas).
# Ignora tombstones (valor "null").

import json
import sys

OPS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def format_row(row):
    return " ".join(f"{k}={v}" for k, v in row.items())


for line in sys.stdin:
    line = line.strip()
    if not line or line == "null":
        continue
    payload = json.loads(line).get("payload")
    if payload is None:
        continue
    op = OPS.get(payload["op"], payload["op"])
    before = payload.get("before")
    after = payload.get("after")
    row_id = (after or before or {}).get("id", "?")
    print(f"[{op}] id={row_id}")
    if before:
        print(f"  antes:   {format_row(before)}")
    if after:
        print(f"  despues: {format_row(after)}")
    print()

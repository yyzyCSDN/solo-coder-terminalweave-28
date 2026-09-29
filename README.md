# TerminalWeave

TerminalWeave is a dependency-light Python engine for modelling day-to-day container-terminal operations. It covers marine access, berth use, quay cranes, yard placement, truck-gate appointments, customs holds, reefer monitoring, hazardous-goods segregation, equipment maintenance, rail blocks, demurrage billing and transshipment connections.

The package is intentionally usable without a database or web framework. Domain services expose ordinary Python APIs, while the persistence layer supplies an append-only event store and snapshots for embedding in larger systems.

## Quick start

```bash
python -m pip install -e ".[test]"
python -m terminalweave.cli demo
python -m pytest -q
```

Run the small HTTP adapter with `terminalweave serve`. It exposes `GET /health`,
`POST /tide/clearance`, and `POST /billing/demurrage`. The other packages are
library components intended to be composed by an embedding application.

## Design principles

- domain calculations are deterministic for identical inputs;
- time values are timezone-aware UTC datetimes;
- operational state transitions are explicit and validated;
- safety constraints fail closed when required information is absent;
- money uses `Decimal`;
- core domain modules depend only on the Python standard library.

## Main packages

`marine`, `cranes`, `yard`, `gate`, `customs`, `reefer`, `hazmat`, `equipment`, `rail`, `billing`, `transshipment`, `planning`, `persistence`, `reporting`, and `api`.

The `reefer` package also provides `incidents.ReeferIncidentFlow`, which chains
temperature alarms, defrost classification, sensor-blackout detection,
power-headroom scheduling and repair work orders into one continuous handling
flow: it first distinguishes normal defrost cycles and telemetry blackouts from
real thermal risk, then arranges power by circuit margin (hold, connect,
relocate, surge-feasible repack, or queue). Each `(container, anomaly kind)` has
at most one active case, and recovered cases retain their complete timeline.

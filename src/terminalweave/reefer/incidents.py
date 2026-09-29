"""Continuous anomaly handling flow for reefers.

Chains the standalone reefer services into one pipeline:

    signal -> classify (normal defrost / sensor blackout / thermal risk)
           -> schedule by power-headroom
           -> open a repair work order
           -> recover, retaining the full timeline

A ``(container, anomaly kind)`` pair has at most one active case: repeated
signals are appended as timeline events instead of opening a second handling.
Recovered cases move to a history ledger rather than being discarded.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from terminalweave.core.clock import ensure_utc
from terminalweave.core.errors import CapacityError, ValidationError
from .defrost import DefrostClassifier, DefrostEvent
from .monitoring import AlarmState, ReeferMonitor, ReeferPolicy, ReeferReading
from .power import ReeferLoad, ReeferPowerBoard
from .workorders import ReeferWorkBoard, ReeferWorkOrder


class AnomalyKind(str, Enum):
    SENSOR_BLACKOUT = 'sensor_blackout'
    THERMAL_RISK = 'thermal_risk'


class PowerAction(str, Enum):
    NONE = 'none'          # no power rearrangement needed (e.g. sensor fault)
    HELD = 'held'          # stays put, circuit has restart margin
    CONNECTED = 'connected'  # unpowered unit connected to spare capacity
    RELOCATED = 'relocated'  # moved to another circuit
    REBALANCED = 'rebalanced'  # board repacked to make room
    QUEUED = 'queued'      # no capacity anywhere; handling waits


class CaseState(str, Enum):
    ACTIVE = 'active'
    RECOVERED = 'recovered'


# Thermal reasons are ordered worst-first; only the first one opens the case,
# later ones are appended as repeat events.
REASON_SEVERITY = {
    'power_loss': 5,
    'abnormal_defrost': 4,
    'thermal_excursion': 4,
    'sensor_blackout': 3,
}


@dataclass(frozen=True)
class CaseEvent:
    at: datetime
    stage: str
    detail: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, 'at', ensure_utc(self.at))


@dataclass
class AnomalyCase:
    case_id: str
    container_id: str
    kind: AnomalyKind
    opened_at: datetime
    reason: str
    state: CaseState = CaseState.ACTIVE
    power_action: PowerAction = PowerAction.NONE
    circuit_id: str | None = None
    work_order_id: str | None = None
    recovered_at: datetime | None = None
    events: list[CaseEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.opened_at = ensure_utc(self.opened_at)

    def add(self, at: datetime, stage: str, **detail: object) -> CaseEvent:
        event = CaseEvent(at, stage, dict(detail))
        self.events.append(event)
        return event


class ReeferIncidentFlow:
    """Orchestrates alarm, defrost, power-board and work-board services."""

    def __init__(
        self,
        policy: ReeferPolicy,
        power_board: ReeferPowerBoard,
        work_board: ReeferWorkBoard | None = None,
        defrost_classifier: DefrostClassifier | None = None,
        max_reading_gap: timedelta = timedelta(minutes=30),
    ):
        if max_reading_gap.total_seconds() <= 0:
            raise ValidationError('max_reading_gap must be positive')
        self.policy = policy
        self.power_board = power_board
        self.work_board = work_board
        self.defrost_classifier = defrost_classifier or DefrostClassifier()
        self.max_gap = max_reading_gap
        self._monitors: dict[str, ReeferMonitor] = {}
        self._last_seen: dict[str, datetime] = {}
        self._loads: dict[str, ReeferLoad] = {}
        self._defrosts: dict[str, list[tuple[DefrostEvent, str]]] = {}
        self._active: dict[tuple[str, AnomalyKind], AnomalyCase] = {}
        self._resolved_at: dict[tuple[str, AnomalyKind], datetime] = {}
        self._history: list[AnomalyCase] = []
        self.observations: list[CaseEvent] = []
        self._seq = 0

    # -- registration -----------------------------------------------------

    def register_load(self, load: ReeferLoad) -> None:
        """Tell the flow a container's demand profile (used to restore power)."""
        self._loads[load.container_id] = load

    # -- intake ------------------------------------------------------------

    def ingest(self, container_id: str, reading: ReeferReading) -> tuple[AnomalyCase, ...]:
        """Feed one temperature reading; returns the container's active cases."""
        monitor = self._monitors.setdefault(container_id, ReeferMonitor(self.policy))
        previous = self._last_seen.get(container_id)
        gap_exceeded = previous is not None and reading.at - previous > self.max_gap
        state = monitor.ingest(reading)  # also enforces strictly increasing time
        self._last_seen[container_id] = reading.at

        if gap_exceeded and previous is not None:
            # The outage itself happened between readings; reconstruct and
            # immediately close it now that telemetry is flowing again.
            case = self._open_blackout(container_id, previous + self.max_gap, repair=False)
            self._recover(case, reading.at, 'telemetry resumed')

        masked = state != AlarmState.NORMAL and self._masked_by_defrost(container_id, reading.at)
        if state == AlarmState.ACTIVE and not masked:
            reason = 'power_loss' if not reading.power_on else 'thermal_excursion'
            self._raise_thermal(container_id, reading.at, reason)
        elif masked:
            self.observations.append(CaseEvent(
                reading.at, 'defrost_masked',
                {'container_id': container_id, 'temperature_c': reading.temperature_c,
                 'alarm_state': state.value},
            ))
            thermal = self._active.get((container_id, AnomalyKind.THERMAL_RISK))
            if thermal is not None:
                thermal.add(reading.at, 'defrost_masked', temperature_c=reading.temperature_c)

        if state == AlarmState.RECOVERING:
            thermal = self._active.get((container_id, AnomalyKind.THERMAL_RISK))
            if thermal is not None:
                thermal.add(reading.at, 'recovering', temperature_c=reading.temperature_c)
        elif state == AlarmState.NORMAL:
            thermal = self._active.get((container_id, AnomalyKind.THERMAL_RISK))
            if thermal is not None:
                self._recover(thermal, reading.at, 'temperature back within band')

        return tuple(
            c for k, c in self._active.items() if k[0] == container_id
        )

    def record_defrost(self, container_id: str, event: DefrostEvent) -> AnomalyCase | None:
        """Record a completed defrost cycle; abnormal cycles raise a thermal case."""
        label = self.defrost_classifier.classify(event)
        self._defrosts.setdefault(container_id, []).append((event, label))
        if label == 'normal':
            self.observations.append(CaseEvent(
                event.end, 'defrost_normal',
                {'container_id': container_id, 'classification': label},
            ))
            return None
        return self._raise_thermal(container_id, event.end, 'abnormal_defrost',
                                  detail={'defrost': label})

    def tick(self, now: datetime) -> tuple[AnomalyCase, ...]:
        """Heartbeat: detects ongoing blackouts and retries queued power actions."""
        now = ensure_utc(now)
        touched: list[AnomalyCase] = []
        for container_id in sorted(set(self._monitors) | set(self._loads)):
            blackout_key = (container_id, AnomalyKind.SENSOR_BLACKOUT)
            last = self._last_seen.get(container_id)
            onset = last + self.max_gap if last is not None else now
            overdue = now > onset
            # A blackout already resolved for this same silence window must not
            # be reopened; a later silence starts a fresh window from last_seen.
            already_recorded = blackout_key in self._resolved_at and onset <= self._resolved_at[blackout_key]
            if overdue and blackout_key not in self._active and not already_recorded:
                touched.append(self._open_blackout(container_id, onset, repair=True))

            thermal = self._active.get((container_id, AnomalyKind.THERMAL_RISK))
            if thermal is not None and thermal.power_action == PowerAction.QUEUED:
                self._dispatch_power(thermal, now)
                if thermal.power_action != PowerAction.QUEUED:
                    touched.append(thermal)
        return tuple(touched)

    # -- cases -------------------------------------------------------------

    def active(self, container_id: str | None = None) -> tuple[AnomalyCase, ...]:
        cases = self._active.values()
        if container_id is not None:
            cases = (c for c in cases if c.container_id == container_id)
        return tuple(sorted(cases, key=lambda c: (c.container_id, c.opened_at, c.case_id)))

    def history(self, container_id: str | None = None) -> tuple[AnomalyCase, ...]:
        cases = self._history
        if container_id is not None:
            cases = (c for c in cases if c.container_id == container_id)
        return tuple(cases)

    def get_case(self, case_id: str) -> AnomalyCase:
        for case in list(self._active.values()) + self._history:
            if case.case_id == case_id:
                return case
        raise KeyError(case_id)

    # -- internals ----------------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        self._seq += 1
        return f'{prefix}-{self._seq:04d}'

    def _masked_by_defrost(self, container_id: str, at: datetime) -> bool:
        return any(
            label == 'normal' and event.start <= at <= event.end
            for event, label in self._defrosts.get(container_id, ())
        )

    def _open_blackout(self, container_id: str, detected_at: datetime, repair: bool) -> AnomalyCase:
        key = (container_id, AnomalyKind.SENSOR_BLACKOUT)
        case = self._active.get(key)
        if case is not None:
            case.add(detected_at, 'repeat')
            return case
        case = AnomalyCase(self._new_id('IC'), container_id,
                           AnomalyKind.SENSOR_BLACKOUT, detected_at, 'sensor_blackout',
                           power_action=PowerAction.NONE)
        case.add(detected_at, 'detected', reason='sensor_blackout')
        case.add(detected_at, 'classified', kind=AnomalyKind.SENSOR_BLACKOUT.value)
        self._active[key] = case
        if repair:
            self._open_work_order(case, detected_at)
        return case

    def _raise_thermal(self, container_id: str, at: datetime, reason: str,
                       detail: dict[str, object] | None = None) -> AnomalyCase:
        key = (container_id, AnomalyKind.THERMAL_RISK)
        case = self._active.get(key)
        if case is not None:
            case.add(at, 'repeat', reason=reason, **(detail or {}))
            if case.power_action == PowerAction.QUEUED:
                self._dispatch_power(case, at)  # headroom may have changed
            return case
        case = AnomalyCase(self._new_id('IC'), container_id,
                           AnomalyKind.THERMAL_RISK, at, reason)
        case.add(at, 'detected', reason=reason, **(detail or {}))
        case.add(at, 'classified', kind=AnomalyKind.THERMAL_RISK.value, cause=reason)
        self._active[key] = case
        self._dispatch_power(case, at)
        self._open_work_order(case, at)
        return case

    def _open_work_order(self, case: AnomalyCase, at: datetime) -> None:
        if self.work_board is None or case.work_order_id is not None:
            return
        order_id = self._new_id('WO')
        self.work_board.open(ReeferWorkOrder(
            order_id=order_id,
            container_id=case.container_id,
            opened_at=at,
            reason=case.reason,
            severity=REASON_SEVERITY[case.reason],
        ))
        case.work_order_id = order_id
        case.add(at, 'work_order_opened', order_id=order_id,
                 severity=REASON_SEVERITY[case.reason])

    def _recover(self, case: AnomalyCase, at: datetime, note: str) -> None:
        key = (case.container_id, case.kind)
        case.state = CaseState.RECOVERED
        case.recovered_at = ensure_utc(at)
        case.add(at, 'recovered', note=note)
        if case.work_order_id is not None and self.work_board is not None:
            order = self.work_board.orders[case.work_order_id]
            case.add(at, 'repair_state', work_state=order.state.value,
                     resolution=order.resolution)
        self._active.pop(key, None)
        self._resolved_at[key] = ensure_utc(at)
        self._history.append(case)

    # -- power headroom scheduling -----------------------------------------

    def _circuit_of(self, container_id: str) -> tuple[str, ReeferLoad] | None:
        for circuit_id, mapping in self.power_board.loads.items():
            if container_id in mapping:
                return circuit_id, mapping[container_id]
        return None

    def _headroom(self, circuit_id: str) -> float:
        circuit = self.power_board.circuits[circuit_id]
        return round(circuit.usable_kw - self.power_board.used_kw(circuit_id, startup=True), 3)

    def _snapshot_board(self) -> dict[str, dict[str, ReeferLoad]]:
        return {cid: dict(mapping) for cid, mapping in self.power_board.loads.items()}

    def _restore_board(self, snapshot: dict[str, dict[str, ReeferLoad]]) -> None:
        self.power_board.loads = snapshot

    def _emergency_repack(self, loads: list[ReeferLoad]) -> dict[str, list[ReeferLoad]] | None:
        """Bin-pack every unit by its startup surge.

        The board's steady-state allocator cannot guarantee that all units may
        restart simultaneously after a power event, so a thermal emergency uses
        a surge-feasible first-fit-decreasing pack; returns None when no
        assignment exists and the caller must queue the handling.
        """
        placement: dict[str, list[ReeferLoad]] = {cid: [] for cid in self.power_board.circuits}
        used = {cid: 0.0 for cid in self.power_board.circuits}
        ordered = sorted(
            loads,
            key=lambda l: (-l.priority, -(l.demand_kw * l.startup_multiplier), l.container_id),
        )
        for load in ordered:
            surge = load.demand_kw * load.startup_multiplier
            choices = [
                (self.power_board.circuits[cid].usable_kw - (used[cid] + surge), cid)
                for cid in self.power_board.circuits
                if used[cid] + surge <= self.power_board.circuits[cid].usable_kw
            ]
            if not choices:
                return None
            _, cid = min(choices)
            placement[cid].append(load)
            used[cid] += surge
        return placement

    def _apply_repack(self, placement: dict[str, list[ReeferLoad]]) -> None:
        self.power_board.loads = {
            cid: {load.container_id: load for load in loads}
            for cid, loads in placement.items()
        }

    def _dispatch_power(self, case: AnomalyCase, at: datetime) -> None:
        action, circuit, detail = self._plan_power(case.container_id)
        case.power_action = action
        case.circuit_id = circuit
        case.add(at, 'power_scheduled', action=action.value,
                 circuit=circuit, **detail)

    def _plan_power(self, container_id: str) -> tuple[PowerAction, str | None, dict[str, object]]:
        board = self.power_board
        located = self._circuit_of(container_id)

        if located is None:
            load = self._loads.get(container_id)
            if load is None:
                return PowerAction.QUEUED, None, {'why': 'load profile unknown'}
            try:
                chosen = board.connect(load)
            except CapacityError:
                loads = [x for m in board.loads.values() for x in m.values()] + [load]
                placement = self._emergency_repack(loads)
                if placement is None:
                    return PowerAction.QUEUED, None, {'why': 'no capacity even after repack'}
                self._apply_repack(placement)
                chosen = self._circuit_of(container_id)[0]
                return PowerAction.REBALANCED, chosen, {'headroom_kw': self._headroom(chosen)}
            return PowerAction.CONNECTED, chosen, {'headroom_kw': self._headroom(chosen)}

        circuit_id, board_load = located
        circuit = board.circuits[circuit_id]
        other_kw = board.used_kw(circuit_id) - board_load.demand_kw
        restart_kw = other_kw + board_load.demand_kw * board_load.startup_multiplier
        if restart_kw <= circuit.usable_kw:
            return PowerAction.HELD, circuit_id, {'headroom_kw': round(circuit.usable_kw - restart_kw, 3)}

        # Restart surge does not fit in place: first try another circuit while
        # everyone keeps running, then a full surge-feasible re-pack.
        snapshot = self._snapshot_board()
        board.disconnect(container_id)
        try:
            chosen = board.connect(board_load)
        except CapacityError:
            self._restore_board(snapshot)
            loads = [x for m in board.loads.values() for x in m.values()]
            placement = self._emergency_repack(loads)
            if placement is None:
                return PowerAction.QUEUED, None, {'why': 'restart margin missing on every circuit'}
            self._apply_repack(placement)
            chosen = self._circuit_of(container_id)[0]
            return PowerAction.REBALANCED, chosen, {'from_circuit': circuit_id,
                                                    'headroom_kw': self._headroom(chosen)}
        return PowerAction.RELOCATED, chosen, {'from_circuit': circuit_id,
                                               'headroom_kw': self._headroom(chosen)}

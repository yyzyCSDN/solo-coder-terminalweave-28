from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from terminalweave.core.clock import ensure_utc
from terminalweave.core.errors import ValidationError, StateTransitionError
from terminalweave.reefer.defrost import DefrostClassifier, DefrostEvent
from terminalweave.reefer.monitoring import AlarmState, ReeferMonitor
from terminalweave.reefer.power import ReeferLoad, ReeferPowerBoard
from terminalweave.reefer.workorders import ReeferWorkBoard, ReeferWorkOrder

class AnomalyKind(str, Enum):
    DEFROST = 'defrost'
    SENSOR_BLACKOUT = 'sensor_blackout'
    THERMAL_RISK = 'thermal_risk'

class Disposition(str, Enum):
    MONITOR = 'monitor'
    REPAIR_SENSOR = 'repair_sensor'
    HOLD_COOLING = 'hold_cooling'
    REALLOCATE_POWER = 'reallocate_power'
    SHED_LOAD = 'shed_load'
    ESCALATE = 'escalate'

class CaseStatus(str, Enum):
    OPEN = 'open'
    RECOVERED = 'recovered'

@dataclass(frozen=True)
class CaseEvent:
    at: datetime
    label: str
    detail: str

    def __post_init__(self):
        object.__setattr__(self, 'at', ensure_utc(self.at))

@dataclass
class DispositionRecord:
    disposition: Disposition
    since: datetime
    severity: int
    power_kw: float
    circuit_id: str | None
    work_order_id: str | None
    active: bool = True

    def __post_init__(self):
        self.since = ensure_utc(self.since)
        if not 1 <= self.severity <= 5:
            raise ValidationError('severity must be 1..5')
        if self.power_kw < 0:
            raise ValidationError('power cannot be negative')

@dataclass
class ReeferAnomalyCase:
    case_id: str
    container_id: str
    opened_at: datetime
    kind: AnomalyKind
    severity: int = 1
    status: CaseStatus = CaseStatus.OPEN
    dispositions: list[DispositionRecord] = field(default_factory=list)
    events: list[CaseEvent] = field(default_factory=list)
    context_end: datetime | None = None
    recovered_at: datetime | None = None
    recovery_detail: str | None = None

    def __post_init__(self):
        self.opened_at = ensure_utc(self.opened_at)
        if self.context_end is not None:
            self.context_end = ensure_utc(self.context_end)
        if self.recovered_at is not None:
            self.recovered_at = ensure_utc(self.recovered_at)

    @property
    def active_disposition(self) -> DispositionRecord | None:
        for d in reversed(self.dispositions):
            if d.active:
                return d
        return None

    def history(self) -> tuple[DispositionRecord, ...]:
        return tuple(self.dispositions)

class ReeferIncidentPipeline:
    """Continuous anomaly flow: triage -> power-aware handling -> recovery.

    Wires the existing reefer components into one process: the
    temperature alarm (:class:`ReeferMonitor`), defrost identification
    (:class:`DefrostClassifier`), power allocation
    (:class:`ReeferPowerBoard`) and repair tracking
    (:class:`ReeferWorkBoard`).

    Triage order is sensor blackout, then defrost signature, then real
    thermal risk, so that the benign look-alikes never escalate. Thermal
    handling is chosen from the live power margin. A container carries at
    most one open case and one active disposition; superseded
    dispositions and recovered cases are retained in full.
    """

    def __init__(
        self,
        power_board: ReeferPowerBoard | None = None,
        work_board: ReeferWorkBoard | None = None,
        defrost_classifier: DefrostClassifier | None = None,
        blackout_after: timedelta = timedelta(minutes=15),
    ):
        if blackout_after.total_seconds() <= 0:
            raise ValidationError('blackout_after must be positive')
        self.power_board = power_board
        self.work_board = work_board
        self.defrost = defrost_classifier or DefrostClassifier()
        self.blackout_after = blackout_after
        self.monitors: dict[str, ReeferMonitor] = {}
        self._active: dict[str, ReeferAnomalyCase] = {}
        self._cases: dict[str, list[ReeferAnomalyCase]] = {}
        self._defrost_seen: dict[str, set[tuple]] = {}
        self._seq = 0

    def register(self, container_id: str, monitor: ReeferMonitor) -> None:
        if container_id in self.monitors:
            raise ValidationError('container already registered')
        self.monitors[container_id] = monitor

    # ------------------------------------------------------------- intake
    def evaluate(
        self,
        container_id: str,
        at: datetime,
        defrost_event: DefrostEvent | None = None,
        load: ReeferLoad | None = None,
    ) -> ReeferAnomalyCase | None:
        at = ensure_utc(at)
        if container_id not in self.monitors:
            raise ValidationError('container not registered')
        case = self._active.get(container_id)
        classification = self.defrost.classify(defrost_event) if defrost_event else None

        # 1) Sensor blackout beats every temperature-based interpretation.
        if self._is_blackout(container_id, at):
            return self._triage(case, container_id, at, AnomalyKind.SENSOR_BLACKOUT,
                                'sensor stream stale beyond blackout window', 3, load)

        alarm = self.monitors[container_id].alarm
        in_window = (
            case is not None
            and case.kind == AnomalyKind.DEFROST
            and case.context_end is not None
            and at <= case.context_end
        )

        benign_cycle = False
        if classification is not None:
            active_cycle = at <= defrost_event.end
            excessive_rise = self.defrost.max_rise < defrost_event.temperature_rise_c
            # A closed cycle is only abnormal when the rise was excessive;
            # its duration no longer threatens the cargo.
            abnormal = classification.startswith('abnormal') and (active_cycle or excessive_rise)
            benign_cycle = not abnormal

            # 2) Abnormal defrost is a genuine thermal event, not a cycle.
            if abnormal:
                severity = 5 if classification == 'abnormal_long_hot' else 4
                return self._triage(case, container_id, at, AnomalyKind.THERMAL_RISK,
                                    f'abnormal defrost: {classification}', severity, load)

            # 3) A normal (or closed long) cycle inside its window suppresses
            #    the temperature alarm. An existing thermal case is never
            #    downgraded by a later cycle.
            if active_cycle and (case is None or case.kind == AnomalyKind.DEFROST):
                case = self._ensure_defrost(case, container_id, defrost_event)
                self._mark_defrost_seen(container_id, defrost_event)
                if alarm.state == AlarmState.ACTIVE:
                    case.events.append(CaseEvent(at, 'observed', 'temperature alarm masked by active defrost window'))
                return case

        # 4) Real alarm: masked only while the defrost window is still open.
        if alarm.state == AlarmState.ACTIVE and not in_window:
            power_lost = bool(alarm.last_reading and not alarm.last_reading.power_on)
            detail = 'temperature alarm active with power lost' if power_lost else 'temperature alarm active beyond policy'
            return self._triage(case, container_id, at, AnomalyKind.THERMAL_RISK, detail, 5, load)

        # 5) No new anomaly: drive open cases toward recovery.
        if case is not None and case.status == CaseStatus.OPEN:
            if case.kind == AnomalyKind.DEFROST:
                if at <= case.context_end:
                    case.events.append(CaseEvent(at, 'observed', f'defrost window open, alarm {alarm.state.value}'))
                elif alarm.state in {AlarmState.NORMAL, AlarmState.RECOVERING}:
                    self._recover(case, at, f'defrost window closed, alarm {alarm.state.value}')
            elif case.kind == AnomalyKind.SENSOR_BLACKOUT:
                self._recover(case, at, f'telemetry resumed, alarm {alarm.state.value}')
            elif case.kind == AnomalyKind.THERMAL_RISK and alarm.state in {AlarmState.NORMAL, AlarmState.RECOVERING}:
                self._recover(case, at, f'temperature back inside clear band, alarm {alarm.state.value}')

        # A completed benign cycle reported after its end is recorded as a
        # full process only when the cargo is already temperature-safe.
        if benign_cycle and not active_cycle and self._is_new_defrost(container_id, defrost_event):
            self._mark_defrost_seen(container_id, defrost_event)
            if case is None and alarm.state != AlarmState.ACTIVE:
                case = self._open(container_id, defrost_event.start, AnomalyKind.DEFROST, 1,
                                  f'expected defrost cycle: {classification}')
                self._apply(case, defrost_event.start, Disposition.MONITOR, 1, 0.0, None, None,
                            'defrost signature recognised; temperature excursion suppressed')
                self._recover(case, defrost_event.end, f'defrost cycle completed: {classification}')
                return case
        elif defrost_event is not None:
            self._mark_defrost_seen(container_id, defrost_event)
        return case

    # ------------------------------------------------------------ triage
    def _triage(self, case: ReeferAnomalyCase | None, container_id: str, at: datetime,
                kind: AnomalyKind, detail: str, severity: int, load: ReeferLoad | None) -> ReeferAnomalyCase:
        if case is None or case.status != CaseStatus.OPEN:
            case = self._open(container_id, at, kind, severity, detail)
        elif case.kind != kind:
            case.events.append(CaseEvent(at, 'reclassified', f'{case.kind.value} -> {kind.value}: {detail}'))
            case.kind = kind
            case.context_end = None
        else:
            case.events.append(CaseEvent(at, 'observed', detail))
        case.severity = max(case.severity, severity)
        disposition, power_kw, circuit_id, note, work_reason = self._plan(kind, severity, load)
        self._apply(case, at, disposition, severity, power_kw, circuit_id, work_reason, note)
        return case

    def _ensure_defrost(self, case: ReeferAnomalyCase | None, container_id: str,
                        event: DefrostEvent) -> ReeferAnomalyCase:
        if case is None or case.status != CaseStatus.OPEN:
            case = self._open(container_id, event.start, AnomalyKind.DEFROST, 1,
                              'expected defrost cycle: normal')
        if case.context_end != event.end:
            case.context_end = event.end
            case.events.append(CaseEvent(event.start, 'defrost_window',
                                         f'defrost active until {event.end.isoformat()}'))
        self._apply(case, event.start, Disposition.MONITOR, 1, 0.0, None, None,
                    'defrost signature recognised; temperature excursion suppressed')
        return case

    def _plan(self, kind: AnomalyKind, severity: int,
              load: ReeferLoad | None) -> tuple[Disposition, float, str | None, str, str | None]:
        if kind == AnomalyKind.DEFROST:
            return Disposition.MONITOR, 0.0, None, 'defrost signature recognised; temperature excursion suppressed', None
        if kind == AnomalyKind.SENSOR_BLACKOUT:
            return Disposition.REPAIR_SENSOR, 0.0, None, 'sensor stream interrupted; verify probe and datalink', 'reefer sensor blackout'
        return self._plan_power(severity, load)

    def _plan_power(self, severity: int,
                    load: ReeferLoad | None) -> tuple[Disposition, float, str | None, str, str | None]:
        board = self.power_board
        if board is None or load is None:
            return Disposition.ESCALATE, 0.0, None, 'thermal risk with no power plan available', 'reefer thermal risk, manual dispatch'
        current = self._circuit_of(load.container_id)
        demand_kw = load.demand_kw * load.startup_multiplier
        # The load's running demand already counts on its current circuit,
        # so only the start-up increment needs additional headroom there.
        surge_kw = max(0.0, demand_kw - load.demand_kw)

        if current is not None and self.margin_kw(current) >= surge_kw:
            return Disposition.HOLD_COOLING, demand_kw, current, \
                f'circuit {current} absorbs {surge_kw:.1f} kW surge', 'reefer thermal risk, hold cooling'
        target = self._circuit_with_margin(load)
        if target is not None:
            board.disconnect(load.container_id)
            self._connect_to(load, target)
            verb = 'moved to' if current is not None else 'connected on'
            return Disposition.REALLOCATE_POWER, demand_kw, target, \
                f'{verb} circuit {target} with {self.margin_kw(target):.1f} kW headroom', \
                'reefer thermal risk, power reallocated'
        if current is not None:
            shed_kw, shed = self._shed(current, surge_kw, load)
            if shed:
                return Disposition.SHED_LOAD, shed_kw, current, \
                    f'freed {shed_kw:.1f} kW on {current} by shedding {sorted(shed)}', \
                    'reefer thermal risk, load shed'
        return Disposition.ESCALATE, 0.0, current, \
            'no circuit margin and insufficient shed-able load', 'reefer thermal risk, capacity exhausted'

    def _circuit_with_margin(self, load: ReeferLoad) -> str | None:
        board = self.power_board
        assert board is not None
        demand_kw = load.demand_kw * load.startup_multiplier
        current = self._circuit_of(load.container_id)
        best: tuple[float, str] | None = None
        for cid, circuit in board.circuits.items():
            if cid == current:
                continue
            # Moving load starts it cold, so project its full surge.
            headroom = circuit.usable_kw - board.used_kw(cid, startup=True)
            if headroom >= demand_kw and (best is None or headroom < best[0]):
                best = (headroom, cid)
        return best[1] if best else None

    def _connect_to(self, load: ReeferLoad, circuit_id: str) -> None:
        board = self.power_board
        assert board is not None
        projected = board.used_kw(circuit_id, startup=True) + load.demand_kw * load.startup_multiplier
        if projected > board.circuits[circuit_id].usable_kw:
            raise StateTransitionError('target circuit lost margin before reconnect')
        board.loads[circuit_id][load.container_id] = load

    def _shed(self, circuit_id: str, surge_kw: float, protected: ReeferLoad) -> tuple[float, list[str]]:
        board = self.power_board
        assert board is not None
        need = surge_kw - self.margin_kw(circuit_id)
        candidates = [x for x in board.loads[circuit_id].values()
                      if x.container_id != protected.container_id and x.priority < protected.priority]
        candidates.sort(key=lambda x: (x.priority, -x.demand_kw, x.container_id))
        freed = 0.0
        shed: list[str] = []
        for x in candidates:
            if freed >= need:
                break
            freed += x.demand_kw
            shed.append(x.container_id)
        if not shed or freed < need:
            return 0.0, []
        for cid in shed:
            del board.loads[circuit_id][cid]
        return freed, shed

    # ---------------------------------------------------------- mutation
    def _open(self, container_id: str, at: datetime, kind: AnomalyKind, severity: int, detail: str) -> ReeferAnomalyCase:
        if container_id in self._active:
            raise StateTransitionError('container already has an active case')
        self._seq += 1
        case = ReeferAnomalyCase(
            case_id=f'RC-{self._seq:04d}',
            container_id=container_id,
            opened_at=at,
            kind=kind,
            severity=severity,
            events=[CaseEvent(at, 'opened', detail)],
        )
        self._active[container_id] = case
        self._cases.setdefault(container_id, []).append(case)
        return case

    def _apply(self, case: ReeferAnomalyCase, at: datetime, disposition: Disposition, severity: int,
               power_kw: float, circuit_id: str | None, work_reason: str | None, note: str) -> None:
        current = case.active_disposition
        if current is not None and current.disposition == disposition and current.circuit_id == circuit_id:
            case.events.append(CaseEvent(at, 'observed', note))
            return
        if current is not None:
            current.active = False
            case.events.append(CaseEvent(at, 'disposition_superseded',
                                         f'{current.disposition.value} -> {disposition.value}: {note}'))
        work_order_id = None
        if work_reason is not None and self.work_board is not None:
            work_order_id = f'WO-{case.case_id}'
            if work_order_id not in self.work_board.orders:
                self.work_board.open(ReeferWorkOrder(
                    order_id=work_order_id,
                    container_id=case.container_id,
                    opened_at=at,
                    reason=work_reason,
                    severity=max(3, severity),
                ))
        record = DispositionRecord(disposition, at, severity, power_kw, circuit_id, work_order_id)
        case.dispositions.append(record)
        case.events.append(CaseEvent(at, f'disposition_{disposition.value}', note))

    def _recover(self, case: ReeferAnomalyCase, at: datetime, detail: str) -> None:
        if case.status != CaseStatus.OPEN:
            raise StateTransitionError('case is not open')
        current = case.active_disposition
        if current is not None:
            current.active = False
        case.status = CaseStatus.RECOVERED
        case.recovered_at = at
        case.recovery_detail = detail
        case.events.append(CaseEvent(at, 'recovered', detail))
        self._active.pop(case.container_id, None)

    # ------------------------------------------------------------ queries
    def margin_kw(self, circuit_id: str) -> float:
        """Steady-state headroom (running loads only, no start-up multipliers)."""
        if self.power_board is None:
            raise ValidationError('power board not configured')
        circuit = self.power_board.circuits[circuit_id]
        return circuit.usable_kw - self.power_board.used_kw(circuit_id, startup=False)

    def active_case(self, container_id: str) -> ReeferAnomalyCase | None:
        return self._active.get(container_id)

    def history(self, container_id: str) -> tuple[ReeferAnomalyCase, ...]:
        return tuple(self._cases.get(container_id, ()))

    def open_cases(self) -> tuple[ReeferAnomalyCase, ...]:
        return tuple(sorted(self._active.values(), key=lambda c: (-c.severity, c.opened_at, c.case_id)))

    # ------------------------------------------------------------ helpers
    def _is_blackout(self, container_id: str, at: datetime) -> bool:
        last = self.monitors[container_id].alarm.last_reading
        return last is not None and at - last.at > self.blackout_after

    def _circuit_of(self, container_id: str) -> str | None:
        if self.power_board is None:
            return None
        for cid, mapping in self.power_board.loads.items():
            if container_id in mapping:
                return cid
        return None

    def _is_new_defrost(self, container_id: str, event: DefrostEvent | None) -> bool:
        if event is None:
            return False
        key = (event.start, event.end, event.temperature_rise_c)
        return key not in self._defrost_seen.get(container_id, set())

    def _mark_defrost_seen(self, container_id: str, event: DefrostEvent) -> None:
        key = (event.start, event.end, event.temperature_rise_c)
        self._defrost_seen.setdefault(container_id, set()).add(key)

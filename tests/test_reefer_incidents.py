from datetime import datetime, timedelta, timezone

import pytest

from terminalweave.reefer.defrost import DefrostEvent
from terminalweave.reefer.incidents import (
    AnomalyCase,
    AnomalyKind,
    CaseState,
    PowerAction,
    ReeferIncidentFlow,
)
from terminalweave.reefer.monitoring import ReeferPolicy, ReeferReading
from terminalweave.reefer.power import PowerCircuit, ReeferLoad, ReeferPowerBoard
from terminalweave.reefer.workorders import ReeferWorkBoard, ReeferWorkState

UTC = timezone.utc
D = datetime(2026, 1, 1, tzinfo=UTC)


def make_flow(policy=None, board=None, work_board=True, gap=timedelta(minutes=30)):
    board = board or ReeferPowerBoard([PowerCircuit('A', 10.0, 0.0)])
    wb = ReeferWorkBoard() if work_board else None
    flow = ReeferIncidentFlow(policy or ReeferPolicy(-18, -10, timedelta(minutes=10),
                                                    timedelta(minutes=5), 0.5),
                              board, wb, max_reading_gap=gap)
    return flow, board, wb


def _excursion_readings(flow, cid, start=D, warm=-2.0, span_minutes=11, power_on=True):
    out = []
    for i in range(span_minutes + 1):
        out.append(flow.ingest(cid, ReeferReading(start + timedelta(minutes=i),
                                                  warm, power_on=power_on)))
    return out


def test_normal_defrost_warm_spell_is_masked_not_an_incident():
    flow, _, _ = make_flow()
    flow.record_defrost('C1', DefrostEvent(D, D + timedelta(minutes=20), 4.0))
    _excursion_readings(flow, 'C1', warm=-2.0)
    assert flow.active() == ()
    stages = [e.stage for e in flow.observations]
    assert stages.count('defrost_normal') == 1
    assert stages.count('defrost_masked') >= 11


def test_abnormal_defrost_raises_real_thermal_case():
    flow, board, wb = make_flow()
    board.connect(ReeferLoad('C1', 5.0))  # steady 5 kW, restart surge 7.5 <= 10
    case = flow.record_defrost('C1', DefrostEvent(D, D + timedelta(minutes=60), 12.0))
    assert case is not None
    assert case.kind == AnomalyKind.THERMAL_RISK and case.reason == 'abnormal_defrost'
    assert case.power_action == PowerAction.HELD and case.circuit_id == 'A'
    order = wb.orders[case.work_order_id]
    assert order.reason == 'abnormal_defrost' and order.severity == 4
    # a subsequent warm excursion is the same case, not a second one
    _excursion_readings(flow, 'C1', start=D + timedelta(minutes=61), warm=-2.0)
    assert flow.active('C1') == (case,)
    stages = [e.stage for e in case.events]
    assert stages.count('detected') == 1
    assert stages.count('repeat') >= 1


def test_tick_detects_sensor_blackout_and_no_duplicate_active_case():
    flow, _, wb = make_flow()
    flow.ingest('C1', ReeferReading(D, -15))
    cases = flow.tick(D + timedelta(minutes=31))
    assert len(cases) == 1
    case = cases[0]
    assert case.kind == AnomalyKind.SENSOR_BLACKOUT
    assert case.power_action == PowerAction.NONE
    order = wb.orders[case.work_order_id]
    assert order.state == ReeferWorkState.OPEN and order.severity == 3
    # repeated heartbeat while still silent must not open another case
    assert flow.tick(D + timedelta(minutes=60)) == ()
    assert len(flow.active()) == 1
    # resume closes the case and keeps the full timeline
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=61), -15))
    assert flow.active() == ()
    history = flow.history('C1')
    assert len(history) == 1 and history[0].state == CaseState.RECOVERED
    stages = [e.stage for e in history[0].events]
    assert stages[:3] == ['detected', 'classified', 'work_order_opened']
    assert stages[-2:] == ['recovered', 'repair_state']
    # the still-open repair order is captured in the retained case


def test_late_reading_gap_self_heals_without_repair_order():
    flow, _, wb = make_flow()
    flow.ingest('C1', ReeferReading(D, -15))
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=90), -15))
    assert flow.active() == ()
    case = flow.history('C1')[0]
    assert case.kind == AnomalyKind.SENSOR_BLACKOUT and case.work_order_id is None
    assert case.opened_at == D + timedelta(minutes=30)
    assert len(wb.orders) == 0


def test_resolved_blackout_window_does_not_reopen_on_next_tick():
    flow, _, _ = make_flow()
    flow.ingest('C1', ReeferReading(D, -15))
    flow.tick(D + timedelta(minutes=40))
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=41), -15))
    assert flow.tick(D + timedelta(minutes=42)) == ()
    # a genuinely later silence opens a new case
    flow.tick(D + timedelta(minutes=72))
    assert len(flow.active('C1')) == 1


def test_thermal_excursion_holds_when_circuit_has_startup_margin():
    board = ReeferPowerBoard([PowerCircuit('A', 10.0, 0.0)])
    flow, board, wb = make_flow(board=board)
    board.connect(ReeferLoad('C1', 6.0, startup_multiplier=1.5))  # 9.0 < 10
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    assert case.reason == 'thermal_excursion'
    assert case.power_action == PowerAction.HELD and case.circuit_id == 'A'
    assert wb.orders[case.work_order_id].severity == 4


def test_unpowered_unit_with_power_loss_gets_connected_then_recovers():
    board = ReeferPowerBoard([PowerCircuit('A', 10.0, 0.1)])
    flow, board, wb = make_flow(board=board)
    flow.register_load(ReeferLoad('C1', 5.0, startup_multiplier=1.5))
    flow.ingest('C1', ReeferReading(D, -15, power_on=True))
    _excursion_readings(flow, 'C1', start=D + timedelta(minutes=1), power_on=False)
    case = flow.active('C1')[0]
    assert case.reason == 'power_loss' and wb.orders[case.work_order_id].severity == 5
    assert case.power_action == PowerAction.CONNECTED and case.circuit_id == 'A'
    # power back, temperature returns through the clear band for clear_after
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=13), -2, power_on=True))
    for minute in range(14, 20):
        flow.ingest('C1', ReeferReading(D + timedelta(minutes=minute), -15, power_on=True))
    recovered = flow.history('C1')[0]
    assert recovered.state == CaseState.RECOVERED
    actions = [e.detail['action'] for e in recovered.events if e.stage == 'power_scheduled']
    assert actions == ['connected']  # no duplicate scheduling on repeats


def test_unit_relocates_when_home_circuit_lacks_restart_margin():
    board = ReeferPowerBoard([PowerCircuit('A', 10.0, 0.0), PowerCircuit('B', 20.0, 0.0)])
    flow, board, _ = make_flow(board=board)
    board.loads['A']['OTHER'] = ReeferLoad('OTHER', 9.0)          # 9 + C1 surge 2 > 10
    board.loads['A']['C1'] = ReeferLoad('C1', 1.0, 2.0)          # steady fits, surge (2) does not
    assert flow._circuit_of('C1')[0] == 'A'
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    assert case.power_action == PowerAction.RELOCATED and case.circuit_id == 'B'
    assert 'C1' not in board.loads['A'] and 'C1' in board.loads['B']


def test_board_is_rebalanced_when_no_single_circuit_accepts_unit():
    board = ReeferPowerBoard([PowerCircuit('A', 10.0, 0.0),
                              PowerCircuit('B', 10.0, 0.0),
                              PowerCircuit('C', 20.0, 0.0)])
    flow, board, _ = make_flow(board=board)
    # C1 is on A with steady 7 kW but a 14 kW restart surge; B and C are
    # blocked for direct relocation, but a surge-feasible repack exists.
    board.loads['A']['C1'] = ReeferLoad('C1', 7.0, 2.0)
    board.loads['B']['XB'] = ReeferLoad('XB', 5.0, 1.0)
    board.loads['B']['XD'] = ReeferLoad('XD', 1.0, 1.0)          # B full (steady)
    board.connect(ReeferLoad('XC', 8.0, 1.0))                    # lands on C
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    assert case.power_action == PowerAction.REBALANCED and case.circuit_id == 'C'
    # every unit is still powered and every circuit is surge-feasible
    placed = {cid for m in board.loads.values() for cid in m}
    assert placed == {'C1', 'XB', 'XD', 'XC'}
    for cid in ('A', 'B', 'C'):
        assert board.used_kw(cid, startup=True) <= board.circuits[cid].usable_kw
    # C1's surge shares C with the smaller units; the repack packed it tight
    assert board.used_kw('C', startup=True) == 20.0


def test_no_capacity_queues_and_tick_retries_when_headroom_frees():
    board = ReeferPowerBoard([PowerCircuit('A', 20.0, 0.0)])
    flow, board, _ = make_flow(board=board)
    flow.register_load(ReeferLoad('C1', 6.0, startup_multiplier=2.0))  # surge 12
    board.connect(ReeferLoad('OTHER', 10.0))                           # 12 + 10 > 20
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    assert case.power_action == PowerAction.QUEUED and case.circuit_id is None
    # still queued on repeat tick while full
    flow.tick(D + timedelta(minutes=30))
    assert case.power_action == PowerAction.QUEUED
    # headroom opens up; next heartbeat schedules the waiting handling
    board.disconnect('OTHER')
    flow.tick(D + timedelta(minutes=31))
    assert case.power_action == PowerAction.CONNECTED and case.circuit_id == 'A'


def test_unknown_load_profile_queues_until_registered():
    flow, _, _ = make_flow()
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    assert case.power_action == PowerAction.QUEUED
    flow.register_load(ReeferLoad('C1', 3.0))
    flow.tick(D + timedelta(minutes=12))
    assert case.power_action == PowerAction.CONNECTED and case.circuit_id == 'A'


def test_blackout_and_thermal_coexist_as_separate_single_active_cases():
    flow, _, _ = make_flow()
    flow.register_load(ReeferLoad('C1', 4.0))
    # warm excursion first
    _excursion_readings(flow, 'C1', start=D, warm=-2.0)
    thermal = flow.active('C1')[0]
    # sensor falls silent during the ongoing thermal handling
    flow.tick(D + timedelta(minutes=42))
    active = {c.kind: c for c in flow.active('C1')}
    assert set(active) == {AnomalyKind.THERMAL_RISK, AnomalyKind.SENSOR_BLACKOUT}
    assert active[AnomalyKind.THERMAL_RISK] is thermal
    # resuming telemetry resolves only the blackout
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=43), -2.0))
    assert flow.active('C1') == (thermal,)
    # cooling down resolves the thermal case; history retains both
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=44), -15.0))
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=50), -15.0))
    assert flow.active('C1') == ()
    assert len(flow.history('C1')) == 2


def test_recovery_waits_for_clear_band_not_first_cool_reading():
    flow, _, _ = make_flow()
    _excursion_readings(flow, 'C1', warm=-2.0)
    case = flow.active('C1')[0]
    # -9.2 is still outside the hysteresis clear band [-17.5, -10.5]
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=12), -9.2))
    assert case.state == CaseState.ACTIVE
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=13), -15))
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=19), -15))
    assert flow.active('C1') == ()
    assert flow.history('C1')[0].recovered_at == D + timedelta(minutes=19)


def test_get_case_and_full_timeline_retained_after_recovery():
    flow, _, wb = make_flow()
    flow.register_load(ReeferLoad('C1', 4.0))
    # readings 0..10: alarm goes ACTIVE at minute 10, no repeats afterwards
    for i in range(11):
        flow.ingest('C1', ReeferReading(D + timedelta(minutes=i), -2.0))
    case = flow.active('C1')[0]
    wb.acknowledge(case.work_order_id, 'tech-7')
    wb.start(case.work_order_id)
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=11), -15))
    flow.ingest('C1', ReeferReading(D + timedelta(minutes=17), -15))
    fetched = flow.get_case(case.case_id)
    assert fetched is case and fetched.state == CaseState.RECOVERED
    stages = [e.stage for e in case.events]
    assert stages == ['detected', 'classified', 'power_scheduled', 'work_order_opened',
                      'recovering', 'recovered', 'repair_state']
    repair = case.events[-1]
    assert repair.detail['work_state'] == 'working' and repair.detail['resolution'] is None
    with pytest.raises(KeyError):
        flow.get_case('nope')

from datetime import datetime,timedelta,timezone
import pytest
from terminalweave.reefer.incidents import (
    ReeferIncidentPipeline,AnomalyKind,Disposition,CaseStatus,
)
from terminalweave.reefer.monitoring import ReeferPolicy,ReeferMonitor,ReeferReading,AlarmState
from terminalweave.reefer.defrost import DefrostEvent
from terminalweave.reefer.power import PowerCircuit,ReeferLoad,ReeferPowerBoard
from terminalweave.reefer.workorders import ReeferWorkBoard
from terminalweave.core.errors import ValidationError

UTC=timezone.utc
D=datetime(2026,1,1,tzinfo=UTC)
POLICY=ReeferPolicy(-1,1,timedelta(minutes=10),timedelta(minutes=5),0.2)

def _pipeline(circuits=None,blackout=timedelta(minutes=15)):
    board=ReeferPowerBoard(circuits or [PowerCircuit("A",20,0.1)])
    work=ReeferWorkBoard()
    return ReeferIncidentPipeline(board,work,blackout_after=blackout),board,work

def _register(pipe,cid):
    m=ReeferMonitor(POLICY)
    pipe.register(cid,m)
    return m

def _drive_active(m,start,minutes=(0,11),temp=4.0):
    for i,mn in enumerate(minutes):
        m.ingest(ReeferReading(start+timedelta(minutes=mn),temp if i else temp))
    return start+timedelta(minutes=minutes[-1])

def test_normal_defrost_masks_active_alarm_then_recovers():
    pipe,_,_=_pipeline()
    m=_register(pipe,"C")
    m.ingest(ReeferReading(D,3.0)); m.ingest(ReeferReading(D+timedelta(minutes=12),3.0))
    assert m.alarm.state==AlarmState.ACTIVE
    case=pipe.evaluate("C",D+timedelta(minutes=12),
                       defrost_event=DefrostEvent(D,D+timedelta(minutes=30),4.0))
    assert case.kind==AnomalyKind.DEFROST
    assert case.active_disposition.disposition==Disposition.MONITOR
    m.ingest(ReeferReading(D+timedelta(minutes=32),0.0))
    case=pipe.evaluate("C",D+timedelta(minutes=32))
    assert case.status==CaseStatus.RECOVERED
    assert case.active_disposition is None

def test_sensor_blackout_beats_defrost_and_repairs():
    pipe,_,work=_pipeline()
    m=_register(pipe,"C")
    m.ingest(ReeferReading(D,0.0))
    case=pipe.evaluate("C",D+timedelta(minutes=20))
    assert case.kind==AnomalyKind.SENSOR_BLACKOUT
    assert case.active_disposition.disposition==Disposition.REPAIR_SENSOR
    assert work.orders["WO-"+case.case_id].severity==3
    m.ingest(ReeferReading(D+timedelta(minutes=21),0.0))
    case=pipe.evaluate("C",D+timedelta(minutes=21))
    assert case.status==CaseStatus.RECOVERED

def test_abnormal_defrost_is_thermal_risk():
    pipe,_,_=_pipeline()
    m=_register(pipe,"C")
    m.ingest(ReeferReading(D,0.0))
    case=pipe.evaluate("C",D+timedelta(minutes=10),
                       defrost_event=DefrostEvent(D,D+timedelta(minutes=60),10.0),
                       load=ReeferLoad("C",5,1.0,3))
    assert case.kind==AnomalyKind.THERMAL_RISK and case.severity==5

def test_closed_long_defrost_with_normal_rise_is_benign():
    pipe,_,_=_pipeline()
    m=_register(pipe,"C")
    m.ingest(ReeferReading(D,0.0)); m.ingest(ReeferReading(D+timedelta(minutes=65),0.0))
    case=pipe.evaluate("C",D+timedelta(minutes=70),
                       defrost_event=DefrostEvent(D,D+timedelta(minutes=60),4.0))
    assert case.kind==AnomalyKind.DEFROST and case.status==CaseStatus.RECOVERED

def test_thermal_holds_when_margin_covers_surge_increment():
    board=ReeferPowerBoard([PowerCircuit("A",10,0.0)])
    pipe=ReeferIncidentPipeline(board,ReeferWorkBoard())
    m=_register(pipe,"C")
    board.loads["A"]["C"]=ReeferLoad("C",4,2.0,5)  # surge increment 4 <= margin 6
    at=_drive_active(m,D)
    case=pipe.evaluate("C",at,load=ReeferLoad("C",4,2.0,5))
    assert case.active_disposition.disposition==Disposition.HOLD_COOLING

def test_thermal_reallocates_to_circuit_with_margin():
    board=ReeferPowerBoard([PowerCircuit("A",12,0.0),PowerCircuit("B",20,0.0)])
    work=ReeferWorkBoard()
    pipe=ReeferIncidentPipeline(board,work)
    m=_register(pipe,"C")
    at=_drive_active(m,D)
    board.connect(ReeferLoad("OTHER",7,1.0,2))
    board.loads["A"]["C"]=ReeferLoad("C",6,2.0,5)
    case=pipe.evaluate("C",at,load=ReeferLoad("C",6,2.0,5))
    assert case.active_disposition.disposition==Disposition.REALLOCATE_POWER
    assert case.active_disposition.circuit_id=="B"
    assert "C" in board.loads["B"] and "C" not in board.loads["A"]
    assert work.orders["WO-"+case.case_id].severity==5

def test_thermal_sheds_lower_priority_load():
    board=ReeferPowerBoard([PowerCircuit("A",10,0.0)])
    pipe=ReeferIncidentPipeline(board,ReeferWorkBoard())
    m=_register(pipe,"C")
    board.loads["A"]["LOW"]=ReeferLoad("LOW",4,1.0,1)
    board.loads["A"]["C"]=ReeferLoad("C",4,2.0,5)  # margin 2 < increment 4
    at=_drive_active(m,D)
    case=pipe.evaluate("C",at,load=ReeferLoad("C",4,2.0,5))
    assert case.active_disposition.disposition==Disposition.SHED_LOAD
    assert "LOW" not in board.loads["A"] and "C" in board.loads["A"]

def test_thermal_escalates_when_no_margin_or_shed_target():
    board=ReeferPowerBoard([PowerCircuit("A",10,0.0)])
    pipe=ReeferIncidentPipeline(board,ReeferWorkBoard())
    m=_register(pipe,"C")
    board.loads["A"]["PEER"]=ReeferLoad("PEER",4,1.0,9)
    board.loads["A"]["C"]=ReeferLoad("C",4,2.0,5)
    at=_drive_active(m,D)
    case=pipe.evaluate("C",at,load=ReeferLoad("C",4,2.0,5))
    assert case.active_disposition.disposition==Disposition.ESCALATE
    assert "PEER" in board.loads["A"]

def test_missing_power_plan_fails_closed_to_escalation():
    pipe=ReeferIncidentPipeline(None,ReeferWorkBoard())
    m=_register(pipe,"C")
    at=_drive_active(m,D)
    case=pipe.evaluate("C",at)
    assert case.active_disposition.disposition==Disposition.ESCALATE

def test_one_active_disposition_per_case_with_supersession():
    pipe,_,_=_pipeline()
    m=_register(pipe,"C")
    m.ingest(ReeferReading(D,3.0)); m.ingest(ReeferReading(D+timedelta(minutes=12),3.0))
    case=pipe.evaluate("C",D+timedelta(minutes=12),
                       defrost_event=DefrostEvent(D,D+timedelta(minutes=30),4.0))
    # telemetry goes silent inside the open window -> blackout reclassifies
    case=pipe.evaluate("C",D+timedelta(minutes=40))
    assert case.kind==AnomalyKind.SENSOR_BLACKOUT
    assert sum(1 for d in case.dispositions if d.active)==1
    assert case.dispositions[0].disposition==Disposition.MONITOR
    assert not case.dispositions[0].active
    labels=[e.label for e in case.events]
    assert "reclassified" in labels and "disposition_superseded" in labels

def test_recovered_process_is_retained_in_full():
    pipe,_,_=_pipeline()
    m=_register(pipe,"C")
    at=_drive_active(m,D)
    case=pipe.evaluate("C",at,load=ReeferLoad("C",5,1.0,3))
    assert pipe.active_case("C") is case
    m.ingest(ReeferReading(D+timedelta(hours=2),0.0))
    m.ingest(ReeferReading(D+timedelta(hours=2,minutes=6),0.0))
    pipe.evaluate("C",D+timedelta(hours=2,minutes=6))
    assert case.status==CaseStatus.RECOVERED
    assert pipe.active_case("C") is None
    assert pipe.history("C")==(case,)
    assert case.recovered_at==D+timedelta(hours=2,minutes=6)
    assert len(case.events)>=3  # opened, disposition, recovered

def test_evaluate_requires_registration():
    pipe,_,_=_pipeline()
    with pytest.raises(ValidationError):
        pipe.evaluate("UNKNOWN",D)

def test_blackout_window_must_be_positive():
    with pytest.raises(ValidationError):
        ReeferIncidentPipeline(blackout_after=timedelta(0))

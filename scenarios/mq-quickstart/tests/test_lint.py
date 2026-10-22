"""zos_lint must reject each defect where the guide (or upstream Kafka) ships it,
and accept what the playbook renders.

The guide fixtures are verbatim excerpts of reference/connect-on-z-quickstart-2025-08.pdf;
the upstream fixtures are apache/kafka 4.1 files.
"""
import pytest

from conftest import FIXTURES

GUIDE = FIXTURES / "guide"
UPSTREAM = FIXTURES / "upstream"


def rules_for(zos_lint, path):
    checked, findings = zos_lint.lint_paths([str(path)])
    assert checked == 1, "linter did not examine %s" % path
    return {f.rule for f in findings}


# (fixture, rules that must fire) -- each rule tied to a defect in the guide
GUIDE_DEFECTS = [
    ("connect-distributed.properties", {"P1", "P3", "P4"}),  # inline comments; rest.port; no listeners
    ("mq-source.properties", {"P1"}),                        # tasks.max=1 # Adjust as needed
    ("connect.env", {"E1", "E2", "E3", "E6", "E7", "E9", "E10"}),  # export/$/quotes/CLASSPATH/G1/heap
    ("start-connect.sh", {"S1"}),                            # KAFKA_OPTS set outside STDENV
    ("KAFCONN.jcl", {"J1", "J3", "J4", "J6"}),               # >71 cols; JUSER/MQHOME; 165-char PARM; SH
    ("racdcert.tso", {"J7"}),                                # ADD('label') DSN(...)
    ("mq-source-connector.json", {"C1"}),                    # {"name","config"} wrapper
]


@pytest.mark.parametrize("name,expected", GUIDE_DEFECTS, ids=[n for n, _ in GUIDE_DEFECTS])
def test_rule_fires_on_guide_excerpt(zos_lint, name, expected):
    fired = rules_for(zos_lint, GUIDE / name)
    assert expected <= fired, "missing %s; fired %s" % (sorted(expected - fired), sorted(fired))


def test_guide_parm_is_165_characters(zos_lint):
    _checked, findings = zos_lint.lint_paths([str(GUIDE / "KAFCONN.jcl")])
    j4 = [f for f in findings if f.rule == "J4"]
    assert len(j4) == 1 and "165 characters" in j4[0].detail


def test_guide_orphan_symbolics_are_named(zos_lint):
    _checked, findings = zos_lint.lint_paths([str(GUIDE / "KAFCONN.jcl")])
    assert sorted(f.detail for f in findings if f.rule == "J3") == ["JUSER", "MQHOME"]


@pytest.mark.parametrize("name,rule", [
    ("kafka-run-class.sh", "S2"),       # #!/bin/bash does not exist on z/OS
    ("connect-distributed.sh", "S2"),
    ("connect-log4j2.yaml", "L1"),      # Console appender without charset IBM-1047
])
def test_rule_fires_on_upstream_file(zos_lint, name, rule):
    assert rule in rules_for(zos_lint, UPSTREAM / name)


# Rules for IBM/Kafka constraints the guide happens not to violate: synthetic known-bad input.
SYNTHETIC = [
    ("p2.properties", "listeners=http://0.0.0.0:8083 \n", "P2"),
    ("p5.properties", "group.id=a\ngroup.id=b\n", "P5"),
    ("e4.env", "JAVA_HOME=/usr/lpp/java/J17.0_64 \n", "E4"),
    ("e5.env", "LOG_DIR=/a\nLOG_DIR=/b\n", "E5"),
    ("e8.env", "KAFKA_OPTS=-Xifa:off\n", "E8"),
    ("e9.env", "KAFKA_JVM_PERFORMANCE_OPTS=-Djava.awt.headless=true\n", "E9"),
    ("s2.sh", "#!/usr/bin/env bash\necho hi\n", "S2"),
    ("j2.jcl", "//STEP1   EXEC PGM=IEFBR14\nSTRAY TEXT\n", "J2"),
    ("j5.jcl", "//STDOUT   DD PATH='/u/kafconn/lo\n//             gs/x.out'\n", "J5"),
    ("j8.jcl", "//TOOLONGNAME EXEC PGM=IEFBR14\n", "J8"),
    ("c2.json", '{"connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector", "tasks.max": 1}', "C2"),
    ("c3.json", '{"connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector", "mq.transport.type": "client"}', "C3"),
    ("c4.json", '{"connector.class": "io.confluent.connect.ibm.mq.IbmMQSourceConnector", "mq.transport.type": "bindings"}', "C4"),
]


@pytest.mark.parametrize("name,text,rule", SYNTHETIC, ids=[r for _, _, r in SYNTHETIC])
def test_rule_fires_on_synthetic_bad_input(zos_lint, tmp_path, name, text, rule):
    path = tmp_path / name
    path.write_text(text)
    assert rule in rules_for(zos_lint, path)


def test_rendered_output_is_clean(zos_lint, rendered):
    checked, findings = zos_lint.lint_paths([
        str(rendered / "rendered"), str(rendered / "jcl"), str(rendered / "connectors"),
        str(rendered / "build" / "confluent-connect-zos" / "bin"),
    ])
    assert checked == 11, "expected 4 rendered + 4 JCL + 1 connector + 2 launcher scripts, got %d" % checked
    assert findings == [], "\n".join(str(f) for f in findings)


def test_nothing_checked_is_not_a_pass(zos_lint, tmp_path, capsys):
    (tmp_path / "README.txt").write_text("not a lintable file\n")
    assert zos_lint.main([str(tmp_path)]) == 2
    assert "0 file(s) checked" in capsys.readouterr().out

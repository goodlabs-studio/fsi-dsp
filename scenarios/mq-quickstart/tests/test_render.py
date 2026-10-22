"""The build play, run for real against fake archives, must produce files that
fix the guide's defects -- and must refuse to build when an input would
reintroduce one."""
import re
import subprocess
import tarfile

from conftest import PLUGIN, TOPDIR, build_fake_archives, run_build


def read(out, rel):
    return (out / rel).read_text()


def test_proc_runs_bash_under_pgm_within_the_parm_limit(rendered):
    jcl = read(rendered, "jcl/KAFCONN.jcl").splitlines()
    i = next(n for n, line in enumerate(jcl) if "PARM='" in line)
    parm = jcl[i].split("PARM='", 1)[1] + jcl[i + 1][15:].rstrip("'")
    assert parm == "PGM /usr/lpp/IBM/foz/v1r1/bin/bash /u/kafconn/bin/start-connect.sh"
    assert len(parm) <= 100
    assert "//STEPLIB  DD DISP=SHR,DSN=MQM.V9R4M0.SCSQAUTH" in jcl
    assert "//         DD DISP=SHR,DSN=MQM.V9R4M0.SCSQANLE" in jcl


def test_stdenv_is_literal_and_sets_kafka_opts_once(rendered):
    env = read(rendered, "rendered/connect.env")
    names = [line.split("=", 1)[0] for line in env.splitlines()]
    assert names.count("KAFKA_OPTS") == 1
    assert "$" not in env and "export" not in env and "CLASSPATH" not in names
    assert "KAFKA_OPTS=-Djavax.net.ssl.trustStore=safkeyringjce://KAFUSER/KAFCONN.RING " \
           "-Djavax.net.ssl.trustStoreType=JCERACFKS" in env.splitlines()
    assert "KAFKA_HEAP_OPTS=-Xms2g -Xmx2g" in env.splitlines()


def test_start_script_sets_nothing_and_parses(rendered):
    script = rendered / "rendered" / "start-connect.sh"
    text = script.read_text()
    assert text.startswith("#!/usr/lpp/IBM/foz/v1r1/bin/bash\n")
    assert not re.search(r"^\s*export\b", text, re.M)
    subprocess.run(["bash", "-n", str(script)], check=True)


def test_worker_uses_listeners_and_prefixed_security(rendered):
    props = read(rendered, "rendered/connect-distributed.properties").splitlines()
    assert "listeners=http://0.0.0.0:8083" in props
    assert not any(p.startswith(("rest.port", "rest.host.name")) for p in props)
    for prefix in ("", "producer.", "consumer."):
        assert prefix + "security.protocol=SASL_SSL" in props
    assert "plugin.path=/u/kafconn/plugins" in props


def test_bundle_keeps_exec_bits_shebangs_and_binary_jars(rendered, tmp_path):
    src_tar, src_zip = build_fake_archives(tmp_path / "ref")
    with tarfile.open(src_tar) as t:
        original_jar = t.extractfile(TOPDIR + "/share/java/kafka/kafka-clients.jar").read()
    raw = (rendered / "connect-zos-bundle.tar").read_bytes()
    assert raw[257:265] == b"ustar\x0000", "first header is not POSIX ustar: %r" % raw[257:265]
    with tarfile.open(rendered / "connect-zos-bundle.tar") as bundle:
        assert not any(m.pax_headers for m in bundle.getmembers()), "bundle carries pax headers"
        for script in ("bin/connect-distributed.sh", "bin/kafka-run-class.sh"):
            member = bundle.getmember(TOPDIR + "/" + script)
            assert member.mode & 0o111, "%s lost its exec bits: %o" % (script, member.mode)
            assert bundle.extractfile(member).readline() == b"#!/usr/lpp/IBM/foz/v1r1/bin/bash\n"
        assert bundle.extractfile(TOPDIR + "/share/java/kafka/kafka-clients.jar").read() == original_jar
        assert bundle.getmember("plugins/" + PLUGIN + "/lib/kafka-connect-ibmmq-zos.jar").isfile()


def test_jvm_default_truststore_drops_keyring(tmp_path):
    proc, out = run_build(tmp_path, "tls_truststore=jvm_default")
    assert proc.returncode == 0, proc.stdout[-3000:]
    assert not (out / "jcl" / "RACFRING.jcl").exists()
    assert "KAFKA_OPTS" not in read(out, "rendered/connect.env")


def test_sink_is_built_when_its_zip_is_given(tmp_path):
    _tar, zipfile_path = build_fake_archives(tmp_path / "sinkzip")
    proc, out = run_build(tmp_path, "mq_sink_connector_zip=%s" % zipfile_path)
    assert proc.returncode == 0, proc.stdout[-3000:]
    sink = read(out, "connectors/mq-zos-sink-settlement.json")
    assert '"connector.class": "io.confluent.connect.jms.IbmMqSinkConnector"' in sink
    assert '"topics": "payments.mq-bridge.v1.settlement"' in sink


def test_placeholder_input_stops_the_build(tmp_path):
    proc, _out = run_build(tmp_path, "mq_queue_manager=<PLACEHOLDER_QMGR>")
    assert proc.returncode != 0
    assert "still has a placeholder: <PLACEHOLDER_QMGR>" in proc.stdout


def test_long_install_path_fails_the_parm_gate(tmp_path):
    proc, _out = run_build(tmp_path, "connect_base_dir=/u/" + "x" * 60)
    assert proc.returncode != 0
    assert re.search(r"J4 .*KAFCONN\.jcl.*PARM= value longer than 100 characters", proc.stdout)

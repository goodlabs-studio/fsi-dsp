"""preflight.sh against a fake z/OS: the good image passes, and each broken
prerequisite fails with its own [FAIL] line and exit 1 -- including answers it
cannot parse, which must never count as a pass."""
import os
import shutil
import stat
import subprocess

import pytest

from conftest import FIXTURES, SCENARIO, build_fake_archives

GOOD = FIXTURES / "zos" / "good"


@pytest.fixture
def zos(tmp_path):
    """A writable copy of the good z/OS answers; tests overwrite single files."""
    d = tmp_path / "zos"
    shutil.copytree(GOOD, d)
    return d


@pytest.fixture(scope="module")
def archives(tmp_path_factory):
    return build_fake_archives(tmp_path_factory.mktemp("dl"))


def run(zos_dir, archives, **overrides):
    fake_bin = FIXTURES / "bin"
    os.chmod(fake_bin / "ssh", os.stat(fake_bin / "ssh").st_mode | stat.S_IXUSR)
    tarball, connector = archives
    env = {
        "PATH": "%s:%s" % (fake_bin, os.environ["PATH"]),
        "FAKE_ZOS_DIR": str(zos_dir),
        "ZOS_HOST": "zos-test.example.com", "ZOS_USER": "KAFUSER",
        "ZOS_PYZ": "/usr/lpp/IBM/cyp/v3r12/pyz", "ZOS_ZOAU": "/usr/lpp/IBM/zoautil",
        "ZOS_JAVA_HOME": "/usr/lpp/java/J17.0_64", "ZOS_BASH": "/usr/lpp/IBM/foz/v1r1/bin/bash",
        "INSTALL_DIR": "/u/kafconn", "MIN_FREE_KB": "5242880",
        "MQ_INSTALL_PATH": "/usr/lpp/mqm/V9R4M0", "MQ_HLQ": "MQM.V9R4M0",
        "MQ_JARS": "com.ibm.mq.allclient.jar",
        "KAFKA_BOOTSTRAP": "pkc-test1.us-east-1.aws.confluent.cloud:9092",
        "CONNECT_TARBALL": str(tarball), "CONNECT_TOPDIR": "confluent-connect-zos",
        "CONNECTOR_ZIPS": str(connector), "ZIIP_CONFIRMED": "no",
    }
    env.update(overrides)
    proc = subprocess.run(["bash", str(SCENARIO / "preflight.sh")], env=env,
                          capture_output=True, text=True, stdin=subprocess.DEVNULL)
    return proc, proc.stdout.splitlines()


def fails(lines):
    return [line for line in lines if line.startswith("[FAIL]")]


def test_good_image_passes_every_check(zos, archives):
    proc, lines = run(zos, archives)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    oks = [line for line in lines if line.startswith("[ ok ]")]
    # 3 controller tools + tarball + zip + ssh + 13 remote answers (2 MQ data sets)
    assert len(oks) == 18, "\n".join(lines)
    assert "=== Result: 18 ok, 0 fail, 0 warn ===" in lines
    assert any("z/OS level — V3R1" in line for line in oks)
    assert any("zIIP — 2 online" in line for line in oks)


def test_every_remote_command_is_tagged_and_carries_the_ibm_env(zos, archives):
    run(zos, archives)
    calls = (zos / "calls.log").read_text().splitlines()
    assert len(calls) == 13
    assert all(c.startswith(": check=") and "_BPXK_AUTOCVT=ON" in c for c in calls)


# (file to overwrite, new content or None to delete, the [FAIL] label expected)
BROKEN = [
    ("uname.out", "z/OS 04.00 02\n", "z/OS level — V2R4"),                       # certified by Confluent, unsupported by the collection
    ("uname.out", "OS/390 SYS1 27.00 04\n", "z/OS level — could not parse"),   # uname without -I
    ("java.out", 'openjdk version "11.0.24" 2024-07-16\nEclipse OpenJ9 VM\n', "Java — /usr/lpp/java/J17.0_64: Java 11"),
    ("java.out", 'java version "1.8.0_421"\nIBM J9 VM\n', "Java — /usr/lpp/java/J17.0_64: Java 8"),
    ("python.out", "Python 3.11.9\n", "IBM Python — 3.11.9"),
    ("zoau.out", "2024/01/01 00:00:00 CUT V1.3.2.0 abc 1 PH1\n", "ZOAU — 1.3.2"),
    ("zoau.out", "2026/01/01 00:00:00 CUT V1.5.0.0 abc 1 PH1\n", "ZOAU — 1.5.0"),
    ("mqlib.out", "com.ibm.mq.allclient.jar\ncom.ibm.mqjms.jar\n", "MQ java/lib — /usr/lpp/mqm/V9R4M0/java/lib lacks libmqjbnd*.so"),
    ("mqlib.out", "libmqjbnd.so\ncom.ibm.mqjms.jar\n", "MQ java/lib — /usr/lpp/mqm/V9R4M0/java/lib lacks com.ibm.mq.allclient.jar"),
    ("dls-SCSQANLE.out", None, "MQ data sets — MQM.V9R4M0.SCSQANLE not found"),
    ("codeset.out", 'codeset="ISO8859-1"\n', "codeset — ISO8859-1"),
    ("disk.out", "writable=yes\ndir=/u/kafconn\nMounted on   Filesystem   Avail/Total\n", "install dir — could not parse"),
    ("disk.out", "writable=yes\ndir=/u\nFilesystem 1024-blocks Used Available Capacity Mounted on\nOMVS.ROOT 100 90 10 90% /u\n",
     "install dir — /u: 10 KB free"),
    ("disk.out", "writable=no\ndir=/u\nFilesystem 1024-blocks Used Available Capacity Mounted on\nOMVS.ROOT 1 1 99999999 1% /u\n",
     "install dir — /u (for /u/kafconn) is not writable"),
    ("tcp.out", None, "outbound Kafka — pkc-test1.us-east-1.aws.confluent.cloud:9092 not reachable"),
    ("ziip.out", "IEE174I 14.32.07 DISPLAY M 543\n0000   +   0000-0001  H   FC00  +N\n", "zIIP — D M=CPU shows no online zIIP"),
    ("ziip.out", None, "zIIP — UNVERIFIABLE"),
    ("bash.out", None, "bash — /usr/lpp/IBM/foz/v1r1/bin/bash missing"),
]


@pytest.mark.parametrize("fname,content,label", BROKEN, ids=["%s:%s" % (f, l.split(" — ")[0]) for f, _c, l in BROKEN])
def test_broken_prerequisite_fails_with_its_own_line(zos, archives, fname, content, label):
    if content is None:
        (zos / fname).unlink()
    else:
        (zos / fname).write_text(content)
    proc, lines = run(zos, archives)
    assert proc.returncode == 1, proc.stdout
    failed = fails(lines)
    assert len(failed) == 1, "\n".join(failed)
    assert failed[0].startswith("[FAIL] " + label), failed[0]


def test_unreachable_host_fails_once_and_skips_the_rest(zos, archives):
    (zos / "ssh.rc").write_text("255")
    (zos / "ssh.out").write_text("ssh: connect to host zos-test.example.com port 22: Connection refused\n")
    proc, lines = run(zos, archives)
    assert proc.returncode == 1
    assert len(fails(lines)) == 1 and fails(lines)[0].startswith("[FAIL] ssh KAFUSER@zos-test.example.com")
    assert len([line for line in lines if line.startswith("[SKIP]")]) == 11


def test_ziip_assertion_downgrades_to_warn_and_says_so(zos, archives):
    (zos / "ziip.out").unlink()
    proc, lines = run(zos, archives, ZIIP_CONFIRMED="yes")
    assert proc.returncode == 0, proc.stdout
    assert any(line.startswith("[WARN] zIIP — not observed") and "ASSERTED" in line for line in lines)


def test_tarball_without_the_documented_layout_fails(zos, tmp_path, archives):
    import tarfile
    bad = tmp_path / "wrong-layout.tar.gz"
    with tarfile.open(bad, "w:gz") as tar:
        tar.add(str(GOOD / "README"), arcname="confluent-connect-8.1.0/README")
    proc, lines = run(zos, archives, CONNECT_TARBALL=str(bad))
    assert proc.returncode == 1
    assert fails(lines)[0].startswith("[FAIL] Connect tarball — missing confluent-connect-zos/bin/connect-distributed.sh")


def test_missing_input_is_a_usage_error(zos, archives):
    proc, _lines = run(zos, archives, ZOS_HOST="")
    assert proc.returncode == 2
    assert "missing: ZOS_HOST" in proc.stderr

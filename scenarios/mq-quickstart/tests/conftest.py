"""Shared fixtures for the mq-quickstart tests.

Run from the scenario directory with the same Python that has ansible-core
(the render test shells out to ansible-playbook next to sys.executable):

    python -m pytest tests -v
"""
import importlib.util
import io
import os
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

SCENARIO = Path(__file__).resolve().parent.parent
FIXTURES = SCENARIO / "tests" / "fixtures"
UPSTREAM = FIXTURES / "upstream"
TOPDIR = "confluent-connect-zos"
PLUGIN = "confluentinc-kafka-connect-ibmmq-zos-12.1.1"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def zos_lint():
    return _load("zos_lint", SCENARIO / "tools" / "zos_lint.py")


@pytest.fixture(scope="session")
def jcl_filter():
    return _load("jcl_filter", SCENARIO / "filter_plugins" / "jcl.py")


def _add(tar, name, data, mode=0o644):
    info = tarfile.TarInfo(name)
    info.size, info.mode = len(data), mode
    tar.addfile(info, io.BytesIO(data))


def build_fake_archives(dest):
    """A Connect on z/OS tarball and an MQ connector zip with the documented
    layout. The launcher scripts are the real apache/kafka 4.1 files, so the
    shebang rewrite is tested against what Kafka actually ships. They are
    stored WITHOUT exec bits so the build's explicit mode is what the bundle
    test sees, on any controller."""
    dest.mkdir(parents=True, exist_ok=True)
    tarball = dest / "confluent-connect-8.1.0-zOS.tar.gz"
    with tarfile.open(tarball, "w:gz") as tar:
        _add(tar, TOPDIR + "/bin/connect-distributed.sh", (UPSTREAM / "connect-distributed.sh").read_bytes(), 0o644)
        _add(tar, TOPDIR + "/bin/kafka-run-class.sh", (UPSTREAM / "kafka-run-class.sh").read_bytes(), 0o644)
        _add(tar, TOPDIR + "/config/connect-distributed.properties", b"bootstrap.servers=localhost:9092\n")
        _add(tar, TOPDIR + "/share/java/kafka/kafka-clients.jar", bytes(range(256)) * 8)
        # A non-ASCII name makes bsdtar's default format write pax headers;
        # only --format=ustar keeps the bundle readable by older z/OS tar.
        _add(tar, TOPDIR + "/share/doc/NOTICE-café.txt", b"notice\n")
    connector = dest / (PLUGIN + ".zip")
    with zipfile.ZipFile(connector, "w") as zf:
        zf.writestr(PLUGIN + "/lib/kafka-connect-ibmmq-zos.jar", bytes(range(256)) * 4)
        zf.writestr(PLUGIN + "/manifest.json", '{"name": "kafka-connect-ibmmq-zos"}')
    return tarball, connector


def run_build(workdir, *extra_vars):
    """Run the build play against the test inventory. Returns (proc, out_dir)."""
    tarball, connector = build_fake_archives(workdir / "downloads")
    playbook = Path(sys.executable).parent / "ansible-playbook"
    cmd = [
        str(playbook), "site.yml",
        "-i", "tests/fixtures/inventory.test.yml",
        "-e", "@tests/fixtures/inputs.test.yml",
        "-e", "connect_tarball=%s" % tarball,
        "-e", "mq_source_connector_zip=%s" % connector,
        "-e", "out_root=%s" % workdir,
        "--tags", "build", "--skip-tags", "preflight",
    ]
    for ev in extra_vars:
        cmd += ["-e", ev]
    env = dict(os.environ, ANSIBLE_NOCOLOR="1", ANSIBLE_STDOUT_CALLBACK="default")
    proc = subprocess.run(cmd, cwd=SCENARIO, env=env, capture_output=True, text=True, stdin=subprocess.DEVNULL)
    return proc, workdir / "out" / "zos-test"


@pytest.fixture(scope="session")
def rendered(tmp_path_factory):
    proc, out = run_build(tmp_path_factory.mktemp("render"))
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-2000:]
    return out

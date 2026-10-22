#!/usr/bin/env python3
# =============================================================================
# zos_lint.py — reject the Connect on Z quickstart's known defects in generated files
# =============================================================================
# Every rule here exists because the original quickstart (reference/*.pdf) ships
# the defect, and each one fails late and far away on z/OS: at S KAFCONN, at
# first connector start, or only when the keyring is read. The build play runs
# this over out/<host>/ before anything is copied to z/OS. tests/test_lint.py
# proves every rule fires on a verbatim excerpt of the guide.
#
#   tools/zos_lint.py PATH [PATH ...]      (files or directories)
#
# Exit: 0 clean, 1 findings, 2 usage error or NOTHING CHECKED (a lint that
# examined zero files is not a pass).
# =============================================================================
import json
import os
import re
import sys

RULES = {
    "P1": "inline '#' comment in a .properties value (Java properties have no inline comments)",
    "P2": "trailing whitespace in a .properties line (it becomes part of the value)",
    "P3": "rest.port / rest.host.name were removed in Apache Kafka 3.0 and are ignored; use listeners=",
    "P4": "distributed worker is missing a required property",
    "P5": "property defined more than once (the last one silently wins)",
    "E1": "STDENV record is not NAME=VALUE starting in column 1 (no export, comments or blank records)",
    "E2": "'$' in STDENV: BPXBATCH does not expand variables, the literal text is passed",
    "E3": "STDENV value starts with a quote: BPXBATCH passes the quotes literally",
    "E4": "trailing blank in STDENV: kept in z/OS UNIX files, becomes part of the value",
    "E5": "STDENV variable defined more than once",
    "E6": "STDENV sets a variable that is wrong here (not read by Connect, or breaks plugin isolation)",
    "E7": "HotSpot-only JVM flag: IBM Semeru/OpenJ9 silently ignores it",
    "E8": "-Xifa:off disables zIIP eligibility, the reason this scenario runs on Z",
    "E9": "STDENV is missing a required variable",
    "E10": "heap flags (-Xms/-Xmx) belong in KAFKA_HEAP_OPTS, not KAFKA_OPTS",
    "S1": "start script sets environment; STDENV must be the only source (the guide set KAFKA_OPTS twice)",
    "S2": "shebang must be an absolute path to bash on z/OS (there is no /bin/bash or env lookup)",
    "J1": "JCL line longer than 71 columns",
    "J2": "JCL statement line does not start with // or /*",
    "J3": "PROC symbolic is declared but never referenced",
    "J4": "PARM= value longer than 100 characters after symbolic substitution",
    "J5": "apostrophe-enclosed value split wrongly (must fill to column 71, resume in column 16)",
    "J6": "BPXBATCH run with SH: /etc/profile and .profile then override STDENV; use PGM",
    "J7": "RACDCERT ADD takes the data set in ADD(...) and the label in WITHLABEL(...); there is no DSN()",
    "J8": "JCL name field longer than 8 characters",
    "C1": "connector config is not a flat JSON object (PUT /connectors/{name}/config takes no wrapper)",
    "C2": "connector config value is not a string",
    "C3": "connector must use mq.transport.type=bindings (the reason this scenario runs on Z)",
    "C4": "connector config is missing a required key",
    "L1": "log4j2 Console appender needs charset IBM-1047 on z/OS (Confluent Connect on z/OS)",
}

WORKER_REQUIRED = (
    "bootstrap.servers", "group.id", "config.storage.topic", "offset.storage.topic",
    "status.storage.topic", "plugin.path", "listeners",
)
STDENV_REQUIRED = (
    "JAVA_HOME", "PATH", "LIBPATH", "KAFKA_HEAP_OPTS", "KAFKA_JVM_PERFORMANCE_OPTS",
    "KAFKA_LOG4J_OPTS", "LOG_DIR",
)
STDENV_FORBIDDEN = ("KAFKA_CONNECT_PLUGINS", "CLASSPATH", "LOG4J_CONFIGURATION_FILE")
HOTSPOT_FLAGS = (
    "-XX:+UseG1GC", "-XX:+UseStringDeduplication", "-XX:MaxGCPauseMillis",
    "-XX:InitiatingHeapOccupancyPercent", "-XX:+ExplicitGCInvokesConcurrent", "-XX:MaxInlineLevel",
)
CONNECTOR_REQUIRED = (
    "connector.class", "mq.queue.manager", "mq.transport.type", "jms.destination.name",
    "confluent.topic.bootstrap.servers",
)
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")


class Finding(object):
    def __init__(self, rule, path, line, detail=""):
        self.rule, self.path, self.line, self.detail = rule, path, line, detail

    def __str__(self):
        msg = RULES[self.rule] + (": " + self.detail if self.detail else "")
        return "%s %s:%s %s" % (self.rule, self.path, self.line, msg)


# --- .properties ---------------------------------------------------------------
def lint_properties(path, text):
    out, seen = [], {}
    for n, raw in enumerate(text.splitlines(), 1):
        if raw != raw.rstrip():
            out.append(Finding("P2", path, n))
        line = raw.strip()
        if not line or line[0] in "#!":
            continue
        parts = re.split(r"\s*[=:]\s*", line, maxsplit=1)
        key = parts[0]
        value = parts[1] if len(parts) > 1 else ""
        if re.search(r"\s#", value):
            out.append(Finding("P1", path, n, "%s=%r" % (key, value)))
        if key in ("rest.port", "rest.host.name"):
            out.append(Finding("P3", path, n, key))
        if key in seen:
            out.append(Finding("P5", path, n, "%s (first on line %d)" % (key, seen[key])))
        seen.setdefault(key, n)
    if os.path.basename(path) == "connect-distributed.properties":
        for key in WORKER_REQUIRED:
            if key not in seen:
                out.append(Finding("P4", path, 0, key))
    return out


# --- STDENV ----------------------------------------------------------------------
def lint_stdenv(path, text):
    out, seen = [], {}
    for n, raw in enumerate(text.splitlines(), 1):
        if raw != raw.rstrip(" "):
            out.append(Finding("E4", path, n))
        record = raw
        if not ENV_NAME.match(record):
            out.append(Finding("E1", path, n, repr(raw[:40])))
            # Keep checking "export NAME=VALUE" lines: the value carries the
            # guide's other defects ($ expansion, quotes, HotSpot flags).
            exported = re.match(r"^export\s+(.*)$", raw)
            if not exported or not ENV_NAME.match(exported.group(1)):
                continue
            record = exported.group(1)
        name, value = record.split("=", 1)
        if "$" in value:
            out.append(Finding("E2", path, n, name))
        if value[:1] in ("'", '"'):
            out.append(Finding("E3", path, n, name))
        if name in seen:
            out.append(Finding("E5", path, n, name))
        seen[name] = value
        if name in STDENV_FORBIDDEN:
            out.append(Finding("E6", path, n, name))
        for flag in HOTSPOT_FLAGS:
            if flag in value:
                out.append(Finding("E7", path, n, "%s in %s" % (flag, name)))
        if "-Xifa:off" in value:
            out.append(Finding("E8", path, n, name))
        if name == "KAFKA_OPTS" and re.search(r"-Xm[sx]", value):
            out.append(Finding("E10", path, n))
    for name in STDENV_REQUIRED:
        if name not in seen:
            out.append(Finding("E9", path, 0, name))
    if "KAFKA_JVM_PERFORMANCE_OPTS" in seen and "-Xgcpolicy:" not in seen["KAFKA_JVM_PERFORMANCE_OPTS"]:
        out.append(Finding("E9", path, 0, "KAFKA_JVM_PERFORMANCE_OPTS without -Xgcpolicy: keeps kafka-run-class's G1 defaults"))
    return out


# --- shell scripts ---------------------------------------------------------------
SET_ENV = re.compile(r"^\s*(export\s+\w+|(KAFKA_\w+|LIBPATH|CLASSPATH|JAVA_HOME|PATH|STEPLIB)=)")


def lint_script(path, text):
    out = []
    lines = text.splitlines()
    shebang = lines[0] if lines else ""
    interp = shebang[2:].strip().split()[0] if shebang.startswith("#!") and shebang[2:].strip() else ""
    if not interp.startswith("/") or interp == "/bin/bash" or interp.endswith("/env") or not interp.endswith("bash"):
        out.append(Finding("S2", path, 1, repr(shebang)))
    if os.path.basename(path) == "start-connect.sh":
        for n, line in enumerate(lines, 1):
            if SET_ENV.match(line):
                out.append(Finding("S1", path, n, line.strip()[:60]))
    return out


# --- JCL -------------------------------------------------------------------------
def _tso_commands(lines):
    """Join TSO continuation lines (trailing + or -) into whole commands."""
    cmds, buf, start = [], "", 0
    for n, line in lines:
        text = line.rstrip()
        if not buf:
            start = n
        if text.endswith(("+", "-")):
            buf += text[:-1] + " "
            continue
        buf += text
        if buf.strip():
            cmds.append((start, buf.strip()))
        buf = ""
    if buf.strip():
        cmds.append((start, buf.strip()))
    return cmds


def lint_tso_commands(path, numbered_lines):
    out = []
    for n, cmd in _tso_commands(numbered_lines):
        upper = cmd.upper()
        if upper.startswith("RACDCERT") and re.search(r"\bADD\(", upper):
            if "WITHLABEL(" not in upper or re.search(r"\bDSN\(", upper):
                out.append(Finding("J7", path, n, cmd[:70]))
    return out


def _token(text):
    """Leading token of text, up to the first blank outside apostrophes."""
    field, inq = [], False
    for ch in text:
        if ch == "'":
            inq = not inq
        if ch == " " and not inq:
            break
        field.append(ch)
    return "".join(field)


def _read_quoted(lines, i, col):
    """Read an apostrophe-enclosed value starting at lines[i][col] (the opening quote).
    Returns (value, last_line_index, problems)."""
    problems, value = [], []
    line, pos = lines[i], col + 1
    while True:
        end = line.find("'", pos)
        if end != -1:
            value.append(line[pos:end])
            return "".join(value), i, problems
        value.append(line[pos:])
        if len(line) != 71:
            problems.append((i, "value line ends in column %d, not 71" % len(line)))
        i += 1
        if i >= len(lines):
            problems.append((i - 1, "unterminated apostrophe"))
            return "".join(value), i - 1, problems
        line = lines[i]
        if not line.startswith("//") or line[2:15].strip() or line[15:16] in ("", " "):
            problems.append((i, "continuation does not resume in column 16"))
        pos = 15


def lint_jcl(path, text):
    out = []
    lines = text.splitlines()
    instream, tso, symbolics, references, parms = False, [], {}, set(), []
    proc_open = False
    for i, line in enumerate(lines):
        n = i + 1
        if len(line) > 71:
            out.append(Finding("J1", path, n, "%d columns" % len(line)))
        if instream:
            if line.startswith("/*") or line.startswith("//"):
                instream = False
            else:
                tso.append((n, line[:72]))
                continue
        if line.startswith("//*") or line.startswith("/*"):
            continue
        if not line.startswith("//"):
            out.append(Finding("J2", path, n, repr(line[:30])))
            continue
        name = line[2:].split(" ", 1)[0]
        if len(name) > 8:
            out.append(Finding("J8", path, n, name))
        fields = line[2:].split()
        is_proc = bool(name) and len(fields) >= 2 and fields[1] == "PROC"
        if is_proc or (proc_open and not name):
            # PROC statement (and its continuations) declares symbolics:
            #   //KAFCONN PROC JUSER='KAFUSER',      <-- comment
            #   //             MQHOME='/usr/lpp/mqm'
            after = line[2:].split("PROC", 1)[1] if is_proc else line[2:]
            opnd = _token(after.lstrip())
            for sym, val in re.findall(r"([A-Z@#$][A-Z0-9@#$]{0,7})=('(?:[^']|'')*'|[^,]*)", opnd):
                symbolics[sym] = val.strip("'")
            proc_open = opnd.endswith(",")
            continue
        proc_open = False
        references.update(re.findall(r"&([A-Z@#$][A-Z0-9@#$]{0,7})", line))
        if re.search(r"\bDD\s+(\*|DATA)", line):
            instream = True
        m = re.search(r"PARM='", line)
        if m:
            value, _last, problems = _read_quoted(lines, i, m.end() - 1)
            parms.append((n, value))
            for pi, detail in problems:
                out.append(Finding("J5", path, pi + 1, "PARM: " + detail))
        for m in re.finditer(r"PATH='", line):
            _value, _last, problems = _read_quoted(lines, i, m.end() - 1)
            for pi, detail in problems:
                out.append(Finding("J5", path, pi + 1, "PATH: " + detail))
    for sym in sorted(symbolics):
        if sym not in references:
            out.append(Finding("J3", path, 0, sym))
    bpxbatch = "PGM=BPXBATCH" in text
    for n, value in parms:
        resolved = value
        for sym, val in symbolics.items():
            resolved = resolved.replace("&%s." % sym, val).replace("&%s" % sym, val)
        if len(resolved) > 100:
            out.append(Finding("J4", path, n, "%d characters" % len(resolved)))
        if bpxbatch and resolved.upper().startswith("SH "):
            out.append(Finding("J6", path, n))
    out.extend(lint_tso_commands(path, tso))
    return out


def lint_tso(path, text):
    return lint_tso_commands(path, list(enumerate(text.splitlines(), 1)))


# --- connector JSON --------------------------------------------------------------
def lint_connector(path, text):
    try:
        cfg = json.loads(text)
    except ValueError as exc:
        return [Finding("C1", path, 0, "not JSON: %s" % exc)]
    if not isinstance(cfg, dict) or "config" in cfg:
        return [Finding("C1", path, 0)]
    out = []
    for key, val in cfg.items():
        if not isinstance(val, str):
            out.append(Finding("C2", path, 0, key))
    if cfg.get("mq.transport.type") != "bindings":
        out.append(Finding("C3", path, 0, repr(cfg.get("mq.transport.type"))))
    required = CONNECTOR_REQUIRED + (("topics",) if "Sink" in cfg.get("connector.class", "") else ("kafka.topic",))
    for key in required:
        if key not in cfg:
            out.append(Finding("C4", path, 0, key))
    return out


def lint_log4j2(path, text):
    console = text.split("Console:", 1)[1] if "Console:" in text else ""
    console = re.split(r"\n\s{4}\w+:\n", console, maxsplit=1)[0]
    return [] if re.search(r"charset:\s*\"?IBM-1047\"?", console) else [Finding("L1", path, 0)]


# --- dispatch --------------------------------------------------------------------
def kind_of(path):
    base = os.path.basename(path)
    if base.endswith(".properties"):
        return lint_properties
    if base.endswith(".env"):
        return lint_stdenv
    if base.endswith(".sh"):
        return lint_script
    if base.endswith(".jcl"):
        return lint_jcl
    if base.endswith(".tso"):
        return lint_tso
    if base.endswith(".json"):
        return lint_connector
    if base == "connect-log4j2.yaml":
        return lint_log4j2
    return None


def lint_paths(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            for root, _dirs, names in os.walk(p):
                files.extend(os.path.join(root, f) for f in sorted(names))
        else:
            files.append(p)
    checked, findings = 0, []
    for f in sorted(files):
        fn = kind_of(f)
        if fn is None:
            continue
        with open(f, encoding="utf-8") as fh:
            findings.extend(fn(f, fh.read()))
        checked += 1
    return checked, findings


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__ or "usage: zos_lint.py PATH [PATH ...]", file=sys.stderr)
        return 2
    checked, findings = lint_paths(argv)
    for f in findings:
        print(f)
    print("zos_lint: %d file(s) checked, %d finding(s)" % (checked, len(findings)))
    if checked == 0:
        print("zos_lint: nothing was checked; refusing to report a pass", file=sys.stderr)
        return 2
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

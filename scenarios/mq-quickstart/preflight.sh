#!/usr/bin/env bash
# =============================================================================
# preflight.sh — Connect on Z prerequisites, checked before anything touches z/OS
# =============================================================================
# Most of the real time on this install goes to encoding problems and
# ClassNotFound/UnsatisfiedLink debugging after the PROC is already in place.
# Every check here maps to one of those failures and runs in seconds.
#
# Plain SSH, not Ansible modules: ibm.ibm_zos_core needs IBM Python and ZOAU on
# the target, and those are among the things being checked. Each remote command
# is a one-liner tagged ": check=<name>;" (a no-op in the z/OS shell) so the
# report and the tests can tell them apart.
#
# Normally run by site.yml's preflight play, which sets the environment from
# inputs.yml and the inventory. To run it by hand, export the same variables:
#   ZOS_HOST ZOS_USER ZOS_PYZ ZOS_ZOAU ZOS_JAVA_HOME ZOS_BASH INSTALL_DIR
#   MQ_INSTALL_PATH MQ_HLQ MQ_JARS KAFKA_BOOTSTRAP CONNECT_TARBALL
#   CONNECT_TOPDIR CONNECTOR_ZIPS
#   optional: ZOS_SSH_PORT ZOS_SSH_KEY ZOS_CODESET MIN_FREE_KB ZIIP_CONFIRMED
#
# Exit: 0 all checks pass (WARNs allowed), 1 any FAIL, 2 usage.
# Every unparseable answer is a FAIL. Preflight never passes what it could not read.
# =============================================================================
set -euo pipefail

REQUIRED_VARS="ZOS_HOST ZOS_USER ZOS_PYZ ZOS_ZOAU ZOS_JAVA_HOME ZOS_BASH INSTALL_DIR
MQ_INSTALL_PATH MQ_HLQ MQ_JARS KAFKA_BOOTSTRAP CONNECT_TARBALL CONNECT_TOPDIR CONNECTOR_ZIPS"
missing=""
for var in ${REQUIRED_VARS}; do
  [[ -n "${!var:-}" ]] || missing="${missing} ${var}"
done
if [[ -n "${missing}" ]]; then
  echo "usage: preflight.sh reads its inputs from the environment; missing:${missing}" >&2
  echo "       (site.yml's preflight play sets them from inputs.yml and the inventory)" >&2
  exit 2
fi

ZOS_SSH_PORT="${ZOS_SSH_PORT:-22}"
ZOS_SSH_KEY="${ZOS_SSH_KEY:-}"
ZOS_CODESET="${ZOS_CODESET:-IBM-1047}"
MIN_FREE_KB="${MIN_FREE_KB:-5242880}"
ZIIP_CONFIRMED="${ZIIP_CONFIRMED:-no}"
ZOS_MIN="2.5"        # ibm.ibm_zos_core 2.1 supports z/OS V2R5-V3Rx
JAVA_MIN_MAJOR="17"  # Connect in CP 8.x requires Java 17+
PY_MIN="3.12"        # ZOAU 1.4 requires IBM Python 3.12+
ZOAU_MIN="1.4.1"     # ibm.ibm_zos_core 2.1: ZOAU >= 1.4.1, < 1.5
ZOAU_BELOW="1.5"

n_ok=0
n_fail=0
n_warn=0
ok()   { echo "[ ok ] $1 — $2"; n_ok=$((n_ok + 1)); }
fail() { echo "[FAIL] $1 — $2"; n_fail=$((n_fail + 1)); }
warn() { echo "[WARN] $1 — $2"; n_warn=$((n_warn + 1)); }
skip() { echo "[SKIP] $1 — $2"; }

# ver_ge A B: true when dotted version A >= B (numeric fields only)
ver_ge() {
  local -a a b
  local i x y
  IFS=. read -r -a a <<<"$1"
  IFS=. read -r -a b <<<"$2"
  for ((i = 0; i < ${#a[@]} || i < ${#b[@]}; i++)); do
    x=${a[i]:-0}
    y=${b[i]:-0}
    ((10#${x} > 10#${y})) && return 0
    ((10#${x} < 10#${y})) && return 1
  done
  return 0
}

ssh_opts=(-p "${ZOS_SSH_PORT}" -o BatchMode=yes -o ConnectTimeout=15)
if [[ -n "${ZOS_SSH_KEY}" ]]; then
  ssh_opts+=(-i "${ZOS_SSH_KEY}")
fi
# The environment IBM documents for Python/ZOAU, applied to each command.
remote_env="export _BPXK_AUTOCVT=ON _CEE_RUNOPTS='FILETAG(AUTOCVT,AUTOTAG) POSIX(ON)' \
LIBPATH=${ZOS_ZOAU}/lib:${ZOS_PYZ}/lib:/lib:/usr/lib PATH=${ZOS_ZOAU}/bin:${ZOS_PYZ}/bin:/bin:/usr/bin;"

# zos NAME COMMAND: run COMMAND on z/OS; prints its output, returns its status
zos() {
  # shellcheck disable=SC2029  # expanding on the controller is the point: the values are inputs
  ssh "${ssh_opts[@]}" "${ZOS_USER}@${ZOS_HOST}" ": check=$1; ${remote_env} $2" 2>&1
}

echo "mq-quickstart preflight: ${ZOS_USER}@${ZOS_HOST}:${ZOS_SSH_PORT}"
echo

# --- Controller ------------------------------------------------------------------
for tool in ssh tar unzip; do
  if command -v "${tool}" >/dev/null 2>&1; then
    ok "controller: ${tool}" "$(command -v "${tool}")"
  else
    fail "controller: ${tool}" "not on PATH"
  fi
done

if [[ ! -f "${CONNECT_TARBALL}" ]]; then
  fail "Connect tarball" "${CONNECT_TARBALL} not found"
else
  listing=$'\n'"$(tar -tzf "${CONNECT_TARBALL}" 2>/dev/null || true)"$'\n'
  absent=""
  for path in bin/connect-distributed.sh bin/kafka-run-class.sh; do
    case "${listing}" in
      *$'\n'"${CONNECT_TOPDIR}/${path}"$'\n'*) ;;
      *) absent="${absent} ${CONNECT_TOPDIR}/${path}" ;;
    esac
  done
  case "${listing}" in
    *$'\n'"${CONNECT_TOPDIR}/config/"*) ;;
    *) absent="${absent} ${CONNECT_TOPDIR}/config/" ;;
  esac
  if [[ -z "${absent}" ]]; then
    ok "Connect tarball" "$(basename "${CONNECT_TARBALL}") has ${CONNECT_TOPDIR}/bin and config/"
  else
    fail "Connect tarball" "missing${absent}; is connect_tarball_topdir right for this build?"
  fi
fi

for zip in ${CONNECTOR_ZIPS}; do
  if [[ ! -f "${zip}" ]]; then
    fail "connector zip" "${zip} not found"
  # Here-strings, not pipes, throughout: with pipefail, grep -q closing the
  # pipe early can SIGPIPE the writer and turn a match into a false FAIL.
  elif entries=$(unzip -Z1 "${zip}" 2>/dev/null) && grep -Eq '^[^/]+/lib/[^/]+\.jar$' <<<"${entries}"; then
    ok "connector zip" "$(basename "${zip}") has <plugin>/lib/*.jar"
  else
    fail "connector zip" "$(basename "${zip}") has no <plugin>/lib/*.jar; not a Connect plugin archive"
  fi
done

# --- z/OS ----------------------------------------------------------------------------
if ! out=$(zos ssh 'echo ok') || [[ "${out}" != *ok* ]]; then
  fail "ssh ${ZOS_USER}@${ZOS_HOST}" "unreachable or rejected (BatchMode, key auth): ${out:0:120}"
  for name in "z/OS level" "Java" "bash" "IBM Python" "ZOAU" "MQ java/lib" "MQ data sets" \
              "codeset" "install dir" "outbound Kafka" "zIIP"; do
    skip "${name}" "no SSH session"
  done
else
  ok "ssh ${ZOS_USER}@${ZOS_HOST}" "BatchMode session works"

  # z/OS level: uname -I reports "z/OS RR.00 VV" (e.g. "z/OS 05.00 02" = V2R5)
  if out=$(zos uname 'uname -Isrv') && [[ "${out}" =~ z/OS[[:space:]]+([0-9]+)\.[0-9]+[[:space:]]+([0-9]+) ]]; then
    level="$((10#${BASH_REMATCH[2]})).$((10#${BASH_REMATCH[1]}))"
    if ver_ge "${level}" "${ZOS_MIN}"; then
      ok "z/OS level" "V${level/./R} (${out})"
    else
      fail "z/OS level" "V${level/./R}; ibm.ibm_zos_core 2.1 needs V${ZOS_MIN/./R} or later"
    fi
  else
    fail "z/OS level" "could not parse 'uname -Isrv': ${out:0:80}"
  fi

  # Java: IBM Semeru on z/OS; Connect in CP 8.x requires 17+
  if out=$(zos java "${ZOS_JAVA_HOME}/bin/java -version") &&
     [[ "${out}" =~ version[[:space:]]+\"([0-9]+)(\.([0-9]+))? ]]; then
    major=${BASH_REMATCH[1]}
    [[ "${major}" == "1" ]] && major=${BASH_REMATCH[3]}
    vm="non-OpenJ9 VM"
    [[ "${out}" == *OpenJ9* || "${out}" == *"IBM J9"* ]] && vm="OpenJ9"
    if ((10#${major} >= JAVA_MIN_MAJOR)); then
      ok "Java" "${ZOS_JAVA_HOME}: Java ${major}, ${vm}"
    else
      fail "Java" "${ZOS_JAVA_HOME}: Java ${major}; CP 8.x Connect needs ${JAVA_MIN_MAJOR}+ (IBM Semeru 17 or 21)"
    fi
  else
    fail "Java" "could not run or parse ${ZOS_JAVA_HOME}/bin/java -version: ${out:0:80}"
  fi

  if out=$(zos bash "test -x ${ZOS_BASH} && ${ZOS_BASH} --version") && [[ "${out}" == *bash* ]]; then
    ok "bash" "${ZOS_BASH}: ${out%%$'\n'*}"
  else
    fail "bash" "${ZOS_BASH} missing or not executable; the PROC and Kafka scripts run under it"
  fi

  if out=$(zos python "${ZOS_PYZ}/bin/python3 --version") && [[ "${out}" =~ Python[[:space:]]+([0-9]+\.[0-9]+(\.[0-9]+)?) ]]; then
    pyv=${BASH_REMATCH[1]}
    if ver_ge "${pyv}" "${PY_MIN}"; then
      ok "IBM Python" "${pyv} at ${ZOS_PYZ}"
    else
      fail "IBM Python" "${pyv}; ZOAU 1.4 and ibm.ibm_zos_core 2.1 need ${PY_MIN}+"
    fi
  else
    fail "IBM Python" "could not run ${ZOS_PYZ}/bin/python3: ${out:0:80}"
  fi

  if out=$(zos zoau "${ZOS_ZOAU}/bin/zoaversion") && [[ "${out}" =~ V?([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    zv=${BASH_REMATCH[1]}
    if ver_ge "${zv}" "${ZOAU_MIN}" && ! ver_ge "${zv}" "${ZOAU_BELOW}"; then
      ok "ZOAU" "${zv} at ${ZOS_ZOAU}"
    else
      fail "ZOAU" "${zv}; ibm.ibm_zos_core 2.1 needs >= ${ZOAU_MIN} and < ${ZOAU_BELOW}"
    fi
  else
    fail "ZOAU" "could not run or parse ${ZOS_ZOAU}/bin/zoaversion: ${out:0:80}"
  fi

  # MQ bindings native library + the jars that get symlinked into plugin lib/
  if out=$(zos mqlib "ls ${MQ_INSTALL_PATH}/java/lib"); then
    lacking=""
    grep -Eq '^libmqjbnd[^/]*\.so$' <<<"${out}" || lacking="${lacking} libmqjbnd*.so"
    for jar in ${MQ_JARS}; do
      grep -qxF "${jar}" <<<"${out}" || lacking="${lacking} ${jar}"
    done
    if [[ -z "${lacking}" ]]; then
      ok "MQ java/lib" "${MQ_INSTALL_PATH}/java/lib has libmqjbnd and ${MQ_JARS}"
    else
      fail "MQ java/lib" "${MQ_INSTALL_PATH}/java/lib lacks${lacking}"
    fi
  else
    fail "MQ java/lib" "cannot list ${MQ_INSTALL_PATH}/java/lib: ${out:0:80}"
  fi

  for lib in SCSQAUTH SCSQANLE; do
    dsn="${MQ_HLQ}.${lib}"
    if out=$(zos "dls-${lib}" "dls '${dsn}'") && grep -qiF "${dsn}" <<<"${out}"; then
      ok "MQ data sets" "${dsn} cataloged"
    else
      fail "MQ data sets" "${dsn} not found; the PROC STEPLIB needs it for bindings mode"
    fi
  done

  if out=$(zos codeset 'locale -k codeset') && [[ "${out}" =~ codeset=\"?([A-Za-z0-9-]+) ]]; then
    cs=${BASH_REMATCH[1]}
    if [[ "${cs}" == "${ZOS_CODESET}" ]]; then
      ok "codeset" "${cs}"
    else
      fail "codeset" "${cs}; the kit converts to ${ZOS_CODESET} (set zos_expected_codeset only if you mean it)"
    fi
  else
    fail "codeset" "could not parse 'locale -k codeset': ${out:0:80}"
  fi

  # Install dir: nearest existing ancestor must be writable and have room.
  disk_cmd="d='${INSTALL_DIR}'; while [ ! -d \"\$d\" ]; do d=\$(dirname \"\$d\"); done; \
[ -w \"\$d\" ] && echo writable=yes || echo writable=no; echo dir=\$d; df -kP \"\$d\""
  if out=$(zos disk "${disk_cmd}"); then
    avail=$(awk '/^Filesystem/ { getline; print $4; exit }' <<<"${out}")
    where=$(sed -n 's/^dir=//p' <<<"${out}")
    if [[ "${out}" != *writable=yes* ]]; then
      fail "install dir" "${where:-?} (for ${INSTALL_DIR}) is not writable by ${ZOS_USER}"
    elif [[ ! "${avail}" =~ ^[0-9]+$ ]]; then
      fail "install dir" "could not parse 'df -kP ${where}' output: ${out:0:80}"
    elif ((avail < MIN_FREE_KB)); then
      fail "install dir" "${where}: ${avail} KB free, need ${MIN_FREE_KB} KB"
    else
      ok "install dir" "${where}: ${avail} KB free, writable"
    fi
  else
    fail "install dir" "check failed: ${out:0:80}"
  fi

  # Outbound to the first bootstrap server, from z/OS (the firewall ticket)
  first=${KAFKA_BOOTSTRAP%%,*}
  host=${first%:*}
  port=${first##*:}
  tcp_cmd="${ZOS_PYZ}/bin/python3 -c 'import socket,sys; socket.create_connection((sys.argv[1], int(sys.argv[2])), 10)' ${host} ${port} && echo OPEN"
  if out=$(zos tcp "${tcp_cmd}") && [[ "${out}" == *OPEN* ]]; then
    ok "outbound Kafka" "${host}:${port} reachable from z/OS"
  else
    fail "outbound Kafka" "${host}:${port} not reachable from z/OS; network ticket for outbound ${port}"
  fi

  # zIIP: the premise of the scenario. Checked, never changed (sysprog/licensing decision).
  if out=$(zos ziip "opercmd 'D M=CPU'") && [[ "${out}" == *IEE174I* ]]; then
    ziips=$(grep -cE '(^|[[:space:]])\+I([[:space:]]|$)' <<<"${out}" || true)
    if ((ziips > 0)); then
      ok "zIIP" "${ziips} online (D M=CPU)"
    else
      fail "zIIP" "D M=CPU shows no online zIIP; Connect on Z without zIIP runs on general CPs"
    fi
  elif [[ "${ZIIP_CONFIRMED}" == "yes" ]]; then
    warn "zIIP" "not observed (D M=CPU unavailable); ASSERTED online by preflight_ziip_confirmed"
  else
    fail "zIIP" "UNVERIFIABLE: D M=CPU failed (console authority?); have the sysprog confirm, then set preflight_ziip_confirmed: true"
  fi
fi

echo
echo "=== Result: ${n_ok} ok, ${n_fail} fail, ${n_warn} warn ==="
if ((n_fail > 0)); then
  echo "Fix every [FAIL] before running install. Nothing has been changed on z/OS."
  exit 1
fi
exit 0

# =============================================================================
# jcl.py — Ansible filter: lay out an apostrophe-enclosed JCL value
# =============================================================================
# JCL statements end in column 71 (72 is the continuation indicator, 73-80 the
# sequence field). A PATH= or PARM= value longer than the space left on its line
# must be split the way the z/OS MVS JCL Reference prescribes for parameters in
# apostrophes: fill the line through column 71, then resume the value in column
# 16 of the next "//" line. A split line that stops short of column 71 does not
# end the value; the blanks up to column 71 become part of it. That turns
# /u/kafconn/logs into "/u/kafconn/lo  gs", and the PROC fails at S time, not
# at review time.
#
# Usage in a template:
#   {{ 'PATH' | jcl_quoted(logs_dir ~ '/kafconn.out', head='//STDOUT   DD ', trailer=',') }}
# =============================================================================

LAST_COL = 71
CONT_PREFIX = "//" + " " * 13  # a continued value resumes in column 16
CONT_ROOM = LAST_COL - len(CONT_PREFIX)
MAX_PAD = 4  # keeps a statement-continuation operand at or before column 16


def _layout(lead, value, trailer):
    first = lead + "'"
    room = LAST_COL - len(first)
    if room < 1:
        return None
    tail = "'" + trailer
    if len(value) + len(tail) <= room:
        return [first + value + tail]
    lines = []
    line, rest, room_here = first, value, room
    while len(rest) + len(tail) > room_here:
        if len(rest) < room_here:
            # Too long to close on this line, too short to fill it through
            # column 71: no valid split for this lead. The caller shifts the
            # lead one column and tries again.
            return None
        lines.append(line + rest[:room_here])
        line, rest, room_here = CONT_PREFIX, rest[room_here:], CONT_ROOM
    lines.append(line + rest + tail)
    return lines


def jcl_quoted(keyword, value, head="//         ", trailer=""):
    """Return '<head><keyword>='<value>'<trailer>' as a newline-joined JCL block."""
    if "'" in value:
        raise ValueError("jcl_quoted: apostrophes in %r would need doubling; not supported" % value)
    for pad in range(MAX_PAD + 1):
        lines = _layout(head + " " * pad + keyword + "=", value, trailer)
        if lines is not None:
            return "\n".join(lines)
    raise ValueError("jcl_quoted: no valid JCL layout for %s=%r" % (keyword, value))


class FilterModule(object):
    def filters(self):
        return {"jcl_quoted": jcl_quoted}

"""jcl_quoted must lay out any value so that JCL reads back exactly that value.

The linter's reader (_read_quoted) applies the JCL Reference rule independently:
a split line must end in column 71 and resume in column 16. A layout that reads
back differently, or that the reader flags, is a bug in the filter.
"""
import pytest

HEADS = [("//         ", ""), ("//STDOUT   DD ", ","), ("//STDENV   DD ", ",")]


@pytest.mark.parametrize("head,trailer", HEADS)
def test_every_length_round_trips(jcl_filter, zos_lint, head, trailer):
    for n in range(1, 260):
        value = "/u/" + "".join(chr(ord("a") + i % 26) for i in range(n))
        lines = jcl_filter.jcl_quoted("PATH", value, head=head, trailer=trailer).split("\n")
        assert all(len(line) <= 71 for line in lines), (n, lines)
        got, last, problems = zos_lint._read_quoted(lines, 0, lines[0].index("'"))
        assert problems == [], (n, problems, lines)
        assert got == value, (n, lines)
        assert last == len(lines) - 1
        assert lines[-1].endswith("'" + trailer)


def test_short_value_stays_on_one_line(jcl_filter):
    assert jcl_filter.jcl_quoted("PATH", "/u/k/a.env", head="//STDENV   DD ", trailer=",") == \
        "//STDENV   DD PATH='/u/k/a.env',"


def test_apostrophe_is_refused(jcl_filter):
    with pytest.raises(ValueError):
        jcl_filter.jcl_quoted("PATH", "/u/o'brien/x")

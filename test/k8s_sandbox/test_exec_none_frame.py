"""Regression test: a None frame from the exec websocket must not crash.

`peek_*()` and `read_*()` on the k8s WSClient may both call `update()`, so an
intervening update can drain a channel between a truthy peek and the read. The
read then returns None. Decoding it raised AttributeError inside the exec path,
surfacing as an opaque "Error executing command in Pod" with cause
"'NoneType' object has no attribute 'decode'".

Measured in production: 37-122 such events per 1k model calls across all ten
eval arms (worst arm 1493 events / 12222 calls over ~6h), so this is a
high-frequency cluster-wide fault rather than an edge case.
"""

import inspect
from pathlib import Path

import k8s_sandbox._pod.execute as execute_mod


def _read_loop_source() -> str:
    src = Path(inspect.getfile(execute_mod)).read_text(encoding="utf-8")
    start = src.index("while ws_client.is_open():")
    end = src.index("self._verify_output_limit(stdout, stderr)", start)
    return src[start:end]


def test_stdout_frame_is_guarded_before_decode() -> None:
    """A None stdout frame must never reach the decoding helper."""
    loop = _read_loop_source()
    read_at = loop.index("frame = ws_client.read_stdout()")
    guard_at = loop.index("if frame:", read_at)
    filter_at = loop.index("_filter_sentinel_and_returncode", read_at)
    assert read_at < guard_at < filter_at, (
        "read_stdout() may return None; it must be guarded before being passed "
        "to _filter_sentinel_and_returncode, which decodes it"
    )


def test_stderr_frame_is_guarded_before_append() -> None:
    """A None stderr frame must never be appended into the buffer."""
    loop = _read_loop_source()
    assert "stderr.append(ws_client.read_stderr())" not in loop, (
        "read_stderr() may return None; appending it unguarded pushes None into "
        "the buffer, which fails later at buffer.decode()"
    )
    read_at = loop.index("ws_client.read_stderr()")
    guard_at = loop.index("if stderr_frame:", read_at)
    append_at = loop.index("stderr.append(stderr_frame)", read_at)
    assert read_at < guard_at < append_at


def test_filter_sentinel_still_rejects_none_loudly() -> None:
    """The guard is the fix; the helper should not silently accept None.

    Keeping this strict means a NEW unguarded call site fails fast and visibly
    instead of reintroducing the same opaque cause string.
    """
    import pytest

    from k8s_sandbox._pod.execute import ExecuteOperation

    op = object.__new__(ExecuteOperation)
    with pytest.raises(AttributeError):
        ExecuteOperation._filter_sentinel_and_returncode(op, None)  # type: ignore[arg-type]

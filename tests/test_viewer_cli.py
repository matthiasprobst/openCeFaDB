"""The optional viewer has a clear installation and launch contract."""
import sys
from types import SimpleNamespace

from click.testing import CliRunner

from opencefadb import cli


def test_viewer_missing_dependency_has_install_hint(monkeypatch):
    monkeypatch.setattr(cli, "find_spec", lambda name: None)
    def unexpected_launch(*args, **kwargs):
        raise AssertionError("a missing dependency must not launch a subprocess")
    monkeypatch.setattr("subprocess.run", unexpected_launch)
    result = CliRunner().invoke(cli.graph_viewer, [])
    assert result.exit_code != 0
    assert "opencefadb[viewer]" in result.output
    assert "Traceback" not in result.output


def test_viewer_uses_current_python_and_requested_port(monkeypatch):
    monkeypatch.setattr(cli, "find_spec", lambda name: SimpleNamespace(name=name))
    calls = []
    monkeypatch.setattr("subprocess.run", lambda command, **kwargs: calls.append((command, kwargs)))
    result = CliRunner().invoke(cli.graph_viewer, ["--port", "8573", "--host", "127.0.0.1"])
    assert result.exit_code == 0, result.output
    command, options = calls[0]
    assert command[:3] == [sys.executable, "-m", "streamlit"]
    assert command[-4:] == ["--server.port", "8573", "--server.address", "127.0.0.1"]
    assert options == {"check": True}

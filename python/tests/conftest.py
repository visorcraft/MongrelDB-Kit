"""Shared pytest fixtures for live daemon tests.

Boots a `mongreldb-server` subprocess on an ephemeral port, polls /health,
and yields the base URL. The daemon auto-creates the database directory
(0.43.2+ create-if-not-missing) and auto-checkpoints on SIGTERM shutdown.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest


def _find_server_binary() -> str | None:
    """Locate the mongreldb-server binary.

    Search order:
    1. MONGRELDB_SERVER env var (explicit path)
    2. ~/.cargo/bin/mongreldb-server (cargo install)
    3. Sibling repo target dir
    4. PATH
    5. Download from GitHub releases (v0.64.6 prebuilt Linux x64)
    """
    candidates = [
        os.environ.get("MONGRELDB_SERVER", ""),
        str(Path.home() / ".cargo" / "bin" / "mongreldb-server"),
    ]
    # Sibling MongrelDB repo build
    sibling = Path(__file__).resolve().parents[3] / "mongreldb" / "crates" / "mongreldb-server" / "target" / "release" / "mongreldb-server"
    candidates.append(str(sibling))
    for c in candidates:
        if c and Path(c).exists():
            return c
    found = shutil.which("mongreldb-server")
    if found:
        return found

    # Download from GitHub releases (Linux x64 only)
    import platform
    import stat
    import urllib.request
    if platform.machine() not in ("x86_64", "amd64"):
        return None

    cache_dir = Path(tempfile.gettempdir()) / "mdb-test-server"
    cache_dir.mkdir(parents=True, exist_ok=True)
    binary = cache_dir / "mongreldb-server"
    if binary.exists() and os.access(binary, os.X_OK):
        return str(binary)

    url = "https://github.com/visorcraft/MongrelDB/releases/download/v0.64.6/mongreldb-server-linux-x64"
    try:
        print(f"Downloading mongreldb-server from {url}...")
        urllib.request.urlretrieve(url, binary)
        binary.chmod(binary.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        return str(binary)
    except Exception as e:
        print(f"Failed to download: {e}")
        return None


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def daemon_url():
    """Boot a mongreldb-server subprocess and yield its base URL.

    Skips the entire session if the binary isn't found.
    """
    binary = _find_server_binary()
    if not binary:
        pytest.skip("mongreldb-server binary not found (set MONGRELDB_SERVER env var)")

    tmpdir = tempfile.mkdtemp(prefix="mdb_live_")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    proc = subprocess.Popen(
        [binary, tmpdir, "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for health (max 15s)
    import urllib.request
    deadline = time.time() + 15
    ready = False
    while time.time() < deadline:
        if proc.poll() is not None:
            # Process exited early
            stdout, stderr = proc.communicate()
            shutil.rmtree(tmpdir, ignore_errors=True)
            pytest.skip(f"mongreldb-server exited early: {stderr.decode()}")
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=2) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except Exception:
            time.sleep(0.5)

    if not ready:
        proc.send_signal(signal.SIGKILL)
        proc.communicate()
        shutil.rmtree(tmpdir, ignore_errors=True)
        pytest.skip("mongreldb-server did not become healthy in 15s")

    yield url

    # Graceful shutdown (auto-checkpoint)
    proc.send_signal(signal.SIGTERM)
    try:
        proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
    shutil.rmtree(tmpdir, ignore_errors=True)

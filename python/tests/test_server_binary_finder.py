from pathlib import Path
import platform
import urllib.request

import conftest


def test_find_server_binary_download_fallback(monkeypatch, tmp_path):
    monkeypatch.delenv("MONGRELDB_SERVER", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(conftest.shutil, "which", lambda _name: None)
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(conftest.tempfile, "gettempdir", lambda: str(tmp_path))

    real_exists = Path.exists

    def exists_without_installed_server(path):
        if path.name == "mongreldb-server":
            return False
        return real_exists(path)

    def fake_download(_url, dest):
        dest.write_text("#!/bin/sh\n")
        return str(dest), None

    monkeypatch.setattr(Path, "exists", exists_without_installed_server)
    monkeypatch.setattr(urllib.request, "urlretrieve", fake_download)

    assert conftest._find_server_binary() == str(tmp_path / "mdb-test-server" / "mongreldb-server")

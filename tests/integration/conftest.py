"""Shared fixtures for CLI integration tests against a live RawTree backend."""

import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_URL = os.environ.get("RAWTREE_URL", "http://localhost:3000")
DEFAULT_PASSWORD = "securepassword123"


def _unique_suffix() -> str:
    return f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def assert_ok(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, (
        f"command failed ({result.returncode})\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def parse_stdout_json(result: subprocess.CompletedProcess[str]):
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        pytest.fail(
            "stdout is not valid JSON.\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}\n"
            f"error: {exc}"
        )


@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture(scope="session", autouse=True)
def ensure_backend(base_url: str) -> None:
    """Block until the backend is healthy (up to 120s)."""
    for _ in range(120):
        try:
            response = requests.get(f"{base_url}/health", timeout=2)
            if response.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    pytest.fail("Backend not healthy after 120s")


@pytest.fixture(scope="session")
def rtree_bin() -> str:
    configured = os.environ.get("RTREE_BIN")
    if configured:
        path = Path(configured)
    else:
        path = PROJECT_ROOT / "target" / "release" / "rtree"
    if not path.is_file():
        pytest.skip(f"rtree binary not found at {path}")
    return str(path)


@pytest.fixture()
def cli_home():
    path = tempfile.mkdtemp(prefix="rtree_cli_it_")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture()
def rtree(rtree_bin: str, cli_home: str, base_url: str):
    """Run rtree commands in an isolated HOME with RAWTREE_URL set."""

    def run(*args, input_text=None, env_overrides=None, timeout=60):
        env = os.environ.copy()
        env["HOME"] = cli_home
        env["RAWTREE_URL"] = base_url
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            [rtree_bin, *args],
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )

    return run


@pytest.fixture()
def id_suffix() -> str:
    return _unique_suffix()


@pytest.fixture()
def registered_user(base_url: str, id_suffix: str):
    email = f"cli_it_{id_suffix}@example.com"
    password = DEFAULT_PASSWORD
    response = requests.post(
        f"{base_url}/v1/auth/register",
        json={"email": email, "password": password},
        timeout=15,
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    return {
        "email": email,
        "password": password,
        "token": payload["token"],
        "user_id": payload["user_id"],
    }


@pytest.fixture()
def logged_in_user(rtree, registered_user):
    result = rtree(
        "login",
        "--email",
        registered_user["email"],
        "--password",
        registered_user["password"],
        "--json",
    )
    assert_ok(result)
    payload = parse_stdout_json(result)
    assert payload["status"] == "logged_in"
    return registered_user


@pytest.fixture()
def project_name(rtree, logged_in_user, id_suffix):
    name = f"cli_proj_{id_suffix}"
    result = rtree("project", "create", name, "--json")
    assert_ok(result)
    payload = parse_stdout_json(result)
    assert payload["name"] == name
    return name

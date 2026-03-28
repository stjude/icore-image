"""
Module-scoped Docker container fixtures for test performance.

A single Orthanc/Azurite container is started per test module and shared across
all tests in that module.  Data is cleared between tests so each test starts
with a clean state.

Using module scope (not session scope) so that pytest-xdist workers — which
each run entire modules — get independent containers on independent random
ports with no cross-worker conflicts.
"""

import socket

import pytest

from test_utils import OrthancServer, AzuriteServer


# ---------------------------------------------------------------------------
# Module-scoped containers (started once per test file, reused across tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def shared_orthanc():
    """A single Orthanc container shared across all tests in a module."""
    # Allocate a free port for storescp so parallel xdist workers don't collide
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        storescp_port = s.getsockname()[1]

    server = OrthancServer()
    server.storescp_port = storescp_port
    server.add_modality("TEST_AET", "TEST_AET", "host.docker.internal", storescp_port)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="module")
def shared_azurite():
    """A single Azurite container shared across all tests in a module."""
    server = AzuriteServer()
    server.start()
    yield server
    server.stop()


# ---------------------------------------------------------------------------
# Function-scoped wrappers (clear data between tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def orthanc(shared_orthanc):
    """Per-test Orthanc access.  Data is cleared after each test."""
    yield shared_orthanc
    shared_orthanc.clear_data()


@pytest.fixture(scope="function")
def azurite(shared_azurite):
    """Per-test Azurite access.  Data is cleared after each test."""
    yield shared_azurite
    shared_azurite.clear_data()

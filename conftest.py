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
# Session-scoped containers (started once per test runner)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def shared_orthanc():
    """A single Orthanc container shared across all tests within an xdist worker."""
    server = OrthancServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def shared_azurite():
    """A single Azurite container shared across all tests within an xdist worker."""
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

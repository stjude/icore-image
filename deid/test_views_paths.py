"""Unit tests for the path-sanitization helper used by the iCore views.

The helper lives in deid/pathutils.py (deliberately free of any Django
dependency) so it can be imported and tested without bootstrapping the app.
deid/ is the app's import root, so we add it to sys.path the same way the
running app does.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deid"))

from pathutils import is_path_within_directory  # noqa: E402


def test_file_directly_inside_directory(tmp_path):
    target = tmp_path / "logs"
    target.mkdir()
    assert is_path_within_directory(str(target / "run.txt"), str(target)) is True


def test_nested_file_inside_directory(tmp_path):
    target = tmp_path / "logs"
    (target / "20240101").mkdir(parents=True)
    nested = target / "20240101" / "run.txt"
    assert is_path_within_directory(str(nested), str(target)) is True


def test_directory_itself_is_within(tmp_path):
    target = tmp_path / "logs"
    target.mkdir()
    assert is_path_within_directory(str(target), str(target)) is True


def test_sibling_with_shared_name_prefix_is_rejected(tmp_path):
    """The classic normpath/startswith bypass: '/a/logs-evil' shares the
    '/a/logs' prefix as a string but is NOT inside '/a/logs'."""
    safe = tmp_path / "logs"
    safe.mkdir()
    evil = tmp_path / "logs-evil"
    evil.mkdir()
    assert is_path_within_directory(str(evil / "x.txt"), str(safe)) is False


def test_parent_traversal_is_rejected(tmp_path):
    safe = tmp_path / "logs"
    safe.mkdir()
    escaped = safe / ".." / ".." / "etc" / "passwd"
    assert is_path_within_directory(str(escaped), str(safe)) is False


def test_symlink_escaping_directory_is_rejected(tmp_path):
    """realpath resolves symlinks, so a link inside the directory that points
    outside it must be treated as outside."""
    safe = tmp_path / "safe"
    safe.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")

    link = safe / "link"
    link.symlink_to(outside, target_is_directory=True)

    # link/secret.txt resolves to outside/secret.txt, which is not within safe.
    assert is_path_within_directory(str(link / "secret.txt"), str(safe)) is False


def test_incomparable_paths_return_false(monkeypatch, tmp_path):
    """commonpath raises ValueError for incomparable paths (e.g. different
    Windows drives); the helper must treat that as 'not within'."""
    target = tmp_path / "logs"
    target.mkdir()

    def _raise(_paths):
        raise ValueError("Paths don't have the same drive")

    monkeypatch.setattr("pathutils.os.path.commonpath", _raise)
    assert is_path_within_directory(str(target / "run.txt"), str(target)) is False

"""Filesystem path helpers with no Django dependency, kept separate so they
can be unit-tested without bootstrapping the Django app."""

import os


def is_path_within_directory(path, directory):
    """Return True if ``path`` resolves to a location inside ``directory``.

    Resolves symlinks and ``..`` segments with realpath, then uses commonpath
    so that path-traversal tricks cannot escape ``directory``. This is more
    robust than a normpath/startswith prefix check, which can be fooled by
    sibling directories sharing a name prefix (e.g. ``/a/logs-evil`` vs
    ``/a/logs``) and does not follow symlinks.
    """
    directory_real = os.path.realpath(directory)
    path_real = os.path.realpath(path)
    try:
        return os.path.commonpath([directory_real, path_real]) == directory_real
    except ValueError:
        # commonpath raises if the paths are on different drives (Windows) or
        # otherwise cannot be compared; treat that as outside the directory.
        return False

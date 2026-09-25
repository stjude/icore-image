"""The iCore data directory, defined once.

Kept free of heavy imports so Django settings can use it without pulling in
pandas and the DICOM stack at startup.
"""

import os
import sys


def icore_base_dir() -> str:
    """Root of the iCore data directory (config, logs, appdata).

    On Windows ``~/Documents`` is routinely redirected by OneDrive's Known
    Folder Move, which would sync the sqlite databases (corrupting them under a
    sync scanner) and every PHI working directory to the cloud, so app state
    lives under LOCALAPPDATA there instead.

    Built with ``os.path.join`` on a bare ``~`` rather than
    ``expanduser("~/Documents/iCore")``: the latter returns mixed separators on
    Windows (``C:\\Users\\me/Documents/iCore``), and ``ntpath.commonpath``
    normalises them, so such a base can never compare equal to its own prefix.
    """
    if sys.platform == "win32":
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            return os.path.join(local_appdata, "iCore")
    return os.path.join(os.path.expanduser("~"), "Documents", "iCore")

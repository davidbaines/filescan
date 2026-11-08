"""Keep a click in the console window from pausing filescan.

In the classic Windows console, QuickEdit mode starts a text selection when the
window is clicked, and the program stops at its next line of output until the
selection ends (Enter, Esc or a right-click). A long scan then looks stuck.
filescan turns QuickEdit off while it runs and puts the console's previous mode
back when it exits. Windows Terminal does not pause on selection; there the
change has no visible effect.
"""

from __future__ import annotations

import atexit
import ctypes
import sys

_STD_INPUT_HANDLE = -10
_ENABLE_QUICK_EDIT_MODE = 0x0040
_ENABLE_EXTENDED_FLAGS = 0x0080


def disable_quick_edit() -> bool:
    """Turn QuickEdit off until filescan exits. Returns True if the mode was changed."""
    if sys.platform != "win32":
        return False
    try:
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(_STD_INPUT_HANDLE)
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False  # not a console, e.g. input is redirected
        if not mode.value & _ENABLE_QUICK_EDIT_MODE:
            return False
        previous = mode.value
        if not kernel32.SetConsoleMode(handle, (previous & ~_ENABLE_QUICK_EDIT_MODE) | _ENABLE_EXTENDED_FLAGS):
            return False
        atexit.register(kernel32.SetConsoleMode, handle, previous)
        return True
    except (AttributeError, OSError):
        return False

"""
structure_repository.py

Convenience re-export — structure persistence is handled by scanner_repository
since structures live in scanner.structure_library.
"""

from .scanner_repository import (  # noqa: F401
    write_structures,
    read_structures,
    update_governor_status,
)

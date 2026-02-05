#!/usr/bin/env python3
"""
Simple task identification based on command hash.
"""

import hashlib


def compute_task_id(command):
    """
    Generate unique ID for a task based on its command.

    Args:
        command: Command string

    Returns:
        str: SHA256 hash of the command
    """
    return hashlib.sha256(command.encode('utf-8')).hexdigest()

"""Command line round trip between the preview and execute tools.

The preview tools return the argv as a list (`command`) and as one string
(`command_string`). The execute tools take either form. The string is quoted
with POSIX rules (`shlex.join`) and parsed with the same rules (`shlex.split`),
so backslashes (Windows paths, named instances) and arguments holding spaces
(queries) come back unchanged, whatever the platform: the argv is handed to
`subprocess.run` as a list and never goes through a shell.
"""

import shlex
from typing import Any, List


def join_command(command: List[str]) -> str:
    """Quote an argv into one string that `parse_command` turns back into the same argv."""
    return shlex.join(command)


def parse_command(command: Any) -> List[str]:
    """Return the argv from a list of strings or from a string built by `join_command`.

    Raises ValueError when the value is neither, or when the string cannot be parsed
    (an unclosed quote, for instance).
    """
    if isinstance(command, list):
        if not all(isinstance(part, str) for part in command):
            raise ValueError("every element of the command list must be a string")
        argv = list(command)
    elif isinstance(command, str):
        argv = shlex.split(command)
    else:
        raise ValueError("command must be a string or a list of strings")
    if not argv:
        raise ValueError("the command is empty")
    return argv


# Input schema of the `command` argument of the execute tools.
def command_input_schema(preview_tool: str) -> dict:
    return {
        "anyOf": [
            {"type": "array", "items": {"type": "string"}},
            {"type": "string"},
        ],
        "description": (
            f"The command built by {preview_tool}, with real credentials: preferably its "
            "`command` array, passed unchanged; otherwise its `command_string` (the "
            "'Full command for execution' block), passed unchanged. The string is parsed "
            "with POSIX shell quoting, so do not retype or reformat it."
        ),
    }

"""
CLI error pattern matching for actionable diagnostics.

Maps known error patterns from CLI tool output to user-friendly
messages with specific remediation steps.
"""

import re
from typing import List, Tuple

# Each pattern is (compiled_regex, diagnostic_message)
_ERROR_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (
        re.compile(r"connection refused|could not connect|cannot connect|unable to connect", re.IGNORECASE),
        "Connection refused: the database server is unreachable. "
        "Check the --server format (use host,port for SQL Server, host:port for PostgreSQL/Oracle/MySQL). "
        "Verify the server is running and firewall rules allow the connection.",
    ),
    (
        re.compile(r"timeout|timed out|connection timed out", re.IGNORECASE),
        "Connection timed out. Check that the server address and port are correct, "
        "and that no firewall is blocking the connection. "
        "For long-running operations, increase the timeout via the TIMEOUT environment variable.",
    ),
    (
        re.compile(r"authentication failed|login failed|invalid credentials|password.*incorrect", re.IGNORECASE),
        "Authentication failed. Verify the username and password are correct. "
        "For SQL Server, check if Windows/trusted authentication is required (use trusted_auth=true). "
        "For Oracle, verify the connect string format.",
    ),
    (
        re.compile(r"permission denied|access denied|insufficient privileges|not authorized", re.IGNORECASE),
        "Permission denied. The database user may lack required privileges. "
        "Check that the user has SELECT permission on the source table/schema, "
        "and INSERT/CREATE permission on the target if applicable.",
    ),
    (
        re.compile(r"license|licence|not licensed|license file", re.IGNORECASE),
        "License issue. Provide the license file path via the --license parameter. "
        "The CLI looks for the license file in the current directory by default. "
        "Contact arpe.io for license information.",
    ),
    (
        re.compile(r"out of memory|outofmemory|oom|memory allocation|insufficient memory", re.IGNORECASE),
        "Out of memory. Try reducing --paralleldegree to use fewer threads, "
        "or set --merge false to avoid merging large output files in memory. "
        "For very large tables, consider reducing --batchsize.",
    ),
    (
        re.compile(r"file not found|no such file|path.*not found|directory.*not found", re.IGNORECASE),
        "File or directory not found. Check that the output directory exists and is writable, "
        "and that any referenced config/settings/license files exist at the specified paths.",
    ),
    (
        re.compile(r"table.*not found|table.*does not exist|invalid object name|relation.*does not exist", re.IGNORECASE),
        "Table not found. Verify the table name and schema are correct. "
        "Table names may be case-sensitive depending on the database platform.",
    ),
    (
        re.compile(r"column.*not found|column.*does not exist|invalid column", re.IGNORECASE),
        "Column not found. Check the distribute_key_column name for the parallelism method. "
        "Column names may be case-sensitive depending on the database platform.",
    ),
    (
        re.compile(r"ssl|tls|certificate|cert.*verify", re.IGNORECASE),
        "SSL/TLS error. The database may require an encrypted connection. "
        "Check SSL certificate configuration or connection string parameters for SSL mode.",
    ),
]


def diagnose_cli_error(stdout: str, stderr: str, return_code: int) -> List[str]:
    """Match known CLI error patterns to actionable diagnostic messages.

    Args:
        stdout: Standard output from CLI execution.
        stderr: Standard error from CLI execution.
        return_code: Process exit code.

    Returns:
        List of diagnostic messages. Empty if no known patterns matched.
    """
    combined = f"{stdout}\n{stderr}"
    diagnostics = []
    seen = set()

    for pattern, message in _ERROR_PATTERNS:
        if pattern.search(combined) and message not in seen:
            diagnostics.append(message)
            seen.add(message)

    return diagnostics

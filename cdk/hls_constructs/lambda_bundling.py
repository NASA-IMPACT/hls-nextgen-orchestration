"""Export the Lambda bundle's requirements.txt from the workspace uv.lock.

The Lambda code in ``src/`` is a uv workspace member, so its locked
dependencies live in the root ``uv.lock``. PythonFunction only sees its entry
directory, so the dependencies are exported into it before bundling.
"""

import subprocess
from functools import cache
from pathlib import Path

REQUIREMENTS_NAME = "requirements.txt"
"""The filename PythonFunction looks for in its entry directory."""


@cache
def export_requirements(entry: str, package: str) -> str:
    """Write ``package``'s locked dependencies to ``{entry}/requirements.txt``.

    Fails if ``uv.lock`` is out of date with the workspace's pyproject files.

    Returns
    -------
    str
        Path to the written requirements file.
    """
    destination = Path(entry) / REQUIREMENTS_NAME
    command = [
        "uv",
        "export",
        "--package",
        package,
        "--locked",
        "--no-emit-project",
        "--no-dev",
        "--no-editable",
        "-o",
        str(destination),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except FileNotFoundError as error:
        raise RuntimeError(
            "uv is not on PATH, so the Lambda requirements cannot be exported"
        ) from error
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"failed to export the '{package}' requirements:\n{error.stderr}"
        ) from error
    return str(destination)

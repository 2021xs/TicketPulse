#!/usr/bin/env python3
"""Run real Chrome acceptance and always clean up its disposable database."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    code = 1
    try:
        subprocess.run(['python3', 'scripts/browser_fixture.py', 'start'], cwd=ROOT, check=True)
        code = subprocess.run(['npm', 'run', 'test:browser'], cwd=ROOT).returncode
    finally:
        subprocess.run(['python3', 'scripts/browser_fixture.py', 'stop'], cwd=ROOT, check=True)
    return code


if __name__ == '__main__':
    raise SystemExit(main())

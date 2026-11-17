#!/usr/bin/env python3
"""Real Chrome search acceptance with disposable database and real pipeline."""
import subprocess
from pathlib import Path
import local_kafka as runtime
import search_fixture as fixture

ROOT = Path(__file__).resolve().parents[1]


def main():
    fixture.start()
    try:
        return subprocess.run(['npx','playwright','test','--config=playwright.search.config.js'],cwd=ROOT).returncode
    finally:
        runtime.start()
        fixture.stop()


if __name__=='__main__':
    raise SystemExit(main())

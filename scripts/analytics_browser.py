#!/usr/bin/env python3
"""Real Chrome dashboard acceptance against MySQL/Binlog/Kafka/ES/ClickHouse."""
import subprocess
from pathlib import Path
import analytics_fixture as fixture
import local_analytics as runtime

ROOT=Path(__file__).resolve().parents[1]

def main():
    fixture.start()
    try:return subprocess.run(['npx','playwright','test','--config=playwright.analytics.config.js'],cwd=ROOT).returncode
    finally:
        runtime.start();fixture.stop()

if __name__=='__main__':raise SystemExit(main())

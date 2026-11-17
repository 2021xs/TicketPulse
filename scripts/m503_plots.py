#!/usr/bin/env python3
"""Standalone performance figures from saved measurements only."""
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03/figures'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    rows = json.loads((ROOT / 'artifacts/m5-03/ab/combined-results.json').read_text())
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), constrained_layout=True)
    for ax, metric, label in [(axes[0], 'successful_qps', 'Successful completion QPS'), (axes[1], 'p95_ms', 'HTTP p95 (ms)')]:
        for variant, color in [('original', '#b65e46'), ('selected', '#287a8e')]:
            values, lows, highs = [], [], []
            for c in [10, 20, 50]:
                group = [r[metric] for r in rows if r['mode'] == 'http' and r['tickets'] == 10000 and r['variant'] == variant and r['concurrency'] == c and r[metric] is not None]
                values.append(statistics.mean(group));lows.append(min(group));highs.append(max(group))
            ax.plot([10, 20, 50], values, marker='o', color=color, label=variant)
            ax.fill_between([10, 20, 50], lows, highs, color=color, alpha=.15)
        ax.set_xlabel('Closed-loop concurrency');ax.set_ylabel(label);ax.grid(alpha=.2);ax.legend()
    fig.suptitle('10k tickets: two repeated full-dashboard HTTP runs (4 GiB CK)')
    fig.savefig(OUT / 'http-ab-10k.png', dpi=180)
    fig.savefig(OUT / 'http-ab-10k.pdf')
    plt.close(fig)
    source = ROOT / 'artifacts/m5-03/steady'
    steady = [json.loads((source / name / 'summary.json').read_text()) for name in
              ['http-final-100000-c20', 'http-single-100000-c20', 'http-single-100000-c50']]
    labels = ['Mixed, c20', 'Org1 full, c20', 'Org1 full, c50']
    fig, axes = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    for ax, metric, title, scale in [(axes[0], 'successful_qps', 'Completed QPS', 1),
                                    (axes[1], 'p95_ms', 'HTTP p95 (seconds)', .001),
                                    (axes[2], 'peak_tracked_memory_bytes', 'Sampled tracked memory (GiB)', 1 / 1024**3)]:
        values = [s[metric] * scale for s in steady]
        ax.bar(range(3), values, color=['#287a8e', '#287a8e', '#b65e46'])
        ax.set_xticks(range(3), labels, rotation=15);ax.set_title(title);ax.grid(axis='y', alpha=.2)
        if metric == 'peak_tracked_memory_bytes':
            ax.axhline(4, color='#b65e46', linestyle='--', label='4 GiB server limit');ax.legend(fontsize=8)
    fig.suptitle('100k tickets: 120-second full-dashboard HTTP steady tests')
    fig.savefig(OUT / 'steady-100k.png', dpi=180)
    fig.savefig(OUT / 'steady-100k.pdf')
    plt.close(fig)
    data = {'ab_source': 'artifacts/m5-03/ab/combined-results.json', 'steady_sources': [str((source / name / 'summary.json').relative_to(ROOT)) for name in
            ['http-final-100000-c20', 'http-single-100000-c20', 'http-single-100000-c50']],
            'notes': ['Shading spans the two observed runs, not a confidence interval.',
                      '50-concurrency steady test has errors; low all-response latency is not success capacity.',
                      'Native server memory tracker is not an OS cgroup limit.']}
    (OUT / 'sources.json').write_text(json.dumps(data, indent=2) + '\n')


if __name__ == '__main__':
    main()

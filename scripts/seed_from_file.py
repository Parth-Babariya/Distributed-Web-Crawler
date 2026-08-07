#!/usr/bin/env python3
"""Seed crawler frontier from a newline-delimited URL file."""

import argparse
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'v3'))

from master_v3 import MasterV3


def read_seed_urls(path):
    with open(path, encoding='utf-8') as seed_file:
        return [
            line.strip()
            for line in seed_file
            if line.strip() and not line.lstrip().startswith('#')
        ]


def main():
    parser = argparse.ArgumentParser(description='Seed crawler URLs from a file')
    parser.add_argument('--file', default='seed_urls.txt', help='Path to seed URL file')
    parser.add_argument('--priority', type=float, default=100.0, help='Seed priority')
    args = parser.parse_args()

    urls = read_seed_urls(args.file)
    if not urls:
        print({'seeded': 0, 'file': args.file})
        return

    master = MasterV3()
    master.seed_urls(urls, priority=args.priority)
    print({'seeded': len(urls), 'file': args.file})


if __name__ == '__main__':
    main()

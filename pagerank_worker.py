#!/usr/bin/env python3
"""Convenience entrypoint for the incremental PageRank worker."""

import os
import sys

PROJECT_ROOT = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))

from indexer.pagerank_worker import main


if __name__ == '__main__':
    main()

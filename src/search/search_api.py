#!/usr/bin/env python3
"""FastAPI search service backed by the MongoDB inverted index.

Ranking
-------
1. Text relevance is Okapi BM25 over ``term_postings``:

       idf(t)   = ln(1 + (N - df + 0.5) / (df + 0.5))
       bm25(t)  = idf(t) * tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl))

   multiplied by ``1 + TITLE_BOOST`` when the term appears in the page title
   and by how often the term was repeated in the query.
2. The best ``CANDIDATE_POOL_FACTOR * limit`` pages by BM25 are re-ranked
   with PageRank authority:

       score = bm25 * (1 + AUTHORITY_WEIGHT * ln(1 + rank / PAGERANK_BASE))

   PageRank is unnormalized (see indexer/pagerank_worker.py): a page with no
   in-links has rank 1 - d, so ``PAGERANK_BASE`` maps that page to a
   multiplier of ``1 + AUTHORITY_WEIGHT * ln 2`` and the log keeps
   heavily-linked hubs from swamping text relevance.
"""

import math
import os
import sys
import time
from collections import Counter
from typing import Dict, List, Tuple

from fastapi import FastAPI, Query
from pymongo import MongoClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import CrawlerConfig
from indexer.mapreduce_indexer import MapReduceIndexer

app = FastAPI(title="Distributed Web Crawler Search API")

BM25_K1 = 1.2
BM25_B = 0.75
TITLE_BOOST = 0.5
AUTHORITY_WEIGHT = 0.5
PAGERANK_BASE = 1.0 - float(os.getenv('PAGERANK_DAMPING', '0.85'))
CANDIDATE_POOL_FACTOR = 10
MIN_CANDIDATE_POOL = 100
CORPUS_STATS_TTL_SECONDS = 30.0


class SearchService:
    """Query processor using BM25 text relevance and PageRank authority."""

    def __init__(self, mongodb_uri: str = None, database: str = None):
        self.client = MongoClient(mongodb_uri or CrawlerConfig.get_mongo_url())
        self.db = self.client[database or CrawlerConfig.MONGO_DB]
        self.postings = self.db['term_postings']
        self.documents = self.db['pages_documents']
        self.indexed_documents = self.db['indexed_documents']
        self.pagerank_nodes = self.db['pagerank_nodes']
        self.indexer = MapReduceIndexer(
            mongodb_uri=mongodb_uri or CrawlerConfig.get_mongo_url(),
            database=database or CrawlerConfig.MONGO_DB
        )

    def search(self, query: str, limit: int = 10, mode: str = 'any') -> Dict:
        """Rank pages for ``query``.

        mode='any' returns pages containing at least one query term;
        mode='all' returns only pages containing every query term.
        """
        terms = self.indexer.tokenize(query)
        response = {'query': query, 'terms': terms, 'mode': mode, 'results': []}
        if not terms:
            return response

        doc_count, avg_doc_length = self._corpus_stats()
        doc_freqs = self._document_frequencies(sorted(set(terms)))
        if mode == 'all' and not all(doc_freqs.values()):
            return response
        idf = {
            term: self._idf(max(doc_count, df), df)
            for term, df in doc_freqs.items()
            if df
        }
        if not idf:
            return response

        query_weights = Counter(terms)
        term_weights = {term: query_weights[term] * idf[term] for term in idf}
        pipeline = self._build_term_postings_pipeline(
            term_weights,
            avg_doc_length,
            limit,
            required_terms=len(doc_freqs) if mode == 'all' else 0,
        )
        ranked_docs = list(self.postings.aggregate(
            pipeline,
            allowDiskUse=True,
        ))
        document_text = self._load_document_texts([item['url'] for item in ranked_docs])

        for item in ranked_docs:
            snippet = self._make_snippet(document_text.get(item['url'], ''), terms)
            response['results'].append({
                'url': item['url'],
                'title': item.get('title', ''),
                'score': item.get('score', 0.0),
                'term_score': item.get('term_score', 0.0),
                'authority_score': item.get('authority_score', PAGERANK_BASE),
                'matched_terms': sorted(item.get('matched_terms', [])),
                'snippet': snippet
            })

        return response

    @staticmethod
    def _idf(doc_count: int, doc_freq: int) -> float:
        return math.log(1.0 + (doc_count - doc_freq + 0.5) / (doc_freq + 0.5))

    def _document_frequencies(self, terms: List[str]) -> Dict[str, int]:
        # Counts are served from the unique (term, url) index.
        return {term: self.postings.count_documents({'term': term}) for term in terms}

    def _corpus_stats(self) -> Tuple[int, float]:
        """(document count, average document length), cached briefly.

        Computed from per-URL indexer state rather than a global counter
        document, which every indexing transaction would contend on.
        """
        cached = getattr(self, '_corpus_stats_cache', None)
        now = time.monotonic()
        if cached and now - cached[0] < CORPUS_STATS_TTL_SECONDS:
            return cached[1]

        rows = list(self.indexed_documents.aggregate([
            {'$group': {
                '_id': None,
                'doc_count': {'$sum': 1},
                'avg_doc_length': {'$avg': '$doc_length'},
            }}
        ]))
        doc_count = rows[0]['doc_count'] if rows else 0
        avg_doc_length = (rows[0].get('avg_doc_length') if rows else None) or 1.0
        stats = (max(doc_count, 1), float(avg_doc_length))
        self._corpus_stats_cache = (now, stats)
        return stats

    @staticmethod
    def _switch_on_term(values: Dict[str, float], default: float = 0.0) -> Dict:
        return {
            '$switch': {
                'branches': [
                    {'case': {'$eq': ['$term', term]}, 'then': value}
                    for term, value in sorted(values.items())
                ],
                'default': default,
            }
        }

    def _bm25_stages(self, term_weights: Dict[str, float],
                     avg_doc_length: float) -> List[Dict]:
        tf = {'$ifNull': ['$term_frequency', 0]}
        # Postings written before doc_length existed fall back to the
        # average length, i.e. no length normalization.
        length_ratio = {
            '$divide': [{'$ifNull': ['$doc_length', avg_doc_length]}, avg_doc_length]
        }
        saturation = {
            '$divide': [
                {'$multiply': [tf, BM25_K1 + 1]},
                {'$add': [
                    tf,
                    {'$multiply': [
                        BM25_K1,
                        {'$add': [1 - BM25_B, {'$multiply': [BM25_B, length_ratio]}]},
                    ]},
                ]},
            ]
        }
        title_factor = {'$cond': [{'$eq': ['$in_title', True]}, 1 + TITLE_BOOST, 1]}

        return [
            {'$match': {'term': {'$in': sorted(term_weights)}}},
            {
                '$addFields': {
                    'posting_score': {
                        '$multiply': [
                            self._switch_on_term(term_weights),
                            saturation,
                            title_factor,
                        ]
                    }
                }
            },
        ]

    def _authority_score_stages(self, limit: int) -> List[Dict]:
        candidate_pool = max(int(limit) * CANDIDATE_POOL_FACTOR, MIN_CANDIDATE_POOL)
        return [
            # Two-phase ranking: only the best BM25 candidates pay for the
            # PageRank $lookup, instead of every page matching a common term.
            {'$sort': {'term_score': -1, '_id': 1}},
            {'$limit': candidate_pool},
            {
                '$lookup': {
                    'from': 'pagerank_nodes',
                    'localField': '_id',
                    'foreignField': 'url',
                    'as': 'authority',
                }
            },
            {
                '$addFields': {
                    'authority_score': {
                        '$max': [
                            0.0,
                            {'$ifNull': [
                                {'$arrayElemAt': ['$authority.rank', 0]},
                                PAGERANK_BASE,
                            ]},
                        ]
                    }
                }
            },
            {
                '$addFields': {
                    'score': {
                        '$multiply': [
                            '$term_score',
                            {'$add': [
                                1,
                                {'$multiply': [
                                    AUTHORITY_WEIGHT,
                                    {'$ln': {'$add': [
                                        1,
                                        {'$divide': ['$authority_score', PAGERANK_BASE]},
                                    ]}},
                                ]},
                            ]},
                        ]
                    }
                }
            },
            {'$sort': {'score': -1, 'term_score': -1, '_id': 1}},
            {'$limit': int(limit)},
            {
                '$project': {
                    '_id': 0,
                    'url': '$_id',
                    'title': {'$ifNull': ['$title', '']},
                    'page_id': 1,
                    'score': 1,
                    'term_score': 1,
                    'authority_score': 1,
                    'matched_terms': 1,
                }
            },
        ]

    def _build_term_postings_pipeline(self, term_weights: Dict[str, float],
                                      avg_doc_length: float, limit: int,
                                      required_terms: int = 0) -> List[Dict]:
        pipeline = [
            *self._bm25_stages(term_weights, avg_doc_length),
            {
                '$group': {
                    '_id': '$url',
                    'title': {'$first': '$title'},
                    'page_id': {'$first': '$page_id'},
                    'term_score': {'$sum': '$posting_score'},
                    'matched_terms': {'$addToSet': '$term'},
                }
            },
        ]
        if required_terms:
            pipeline.append({
                '$match': {'$expr': {'$eq': [{'$size': '$matched_terms'}, required_terms]}}
            })
        pipeline.extend(self._authority_score_stages(limit))
        return pipeline

    def _load_document_texts(self, urls: List[str]) -> Dict[str, str]:
        if not urls:
            return {}

        cursor = self.documents.find(
            {'url': {'$in': urls}},
            {'url': 1, 'text': 1},
        )
        return {
            item['url']: item.get('text', '')
            for item in cursor
        }

    def _make_snippet(self, text: str, terms: List[str], size: int = 220) -> str:
        lower_text = text.lower()
        first_hit = min(
            [lower_text.find(term) for term in terms if lower_text.find(term) >= 0],
            default=0
        )
        start = max(first_hit - size // 3, 0)
        snippet = text[start:start + size].strip()
        return " ".join(snippet.split())


search_service = None


def get_search_service() -> SearchService:
    global search_service
    if search_service is None:
        search_service = SearchService()
    return search_service


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/search')
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    mode: str = Query('any', pattern='^(any|all)$'),
):
    return get_search_service().search(q, limit=limit, mode=mode)


def main():
    import uvicorn

    uvicorn.run(
        'search.search_api:app',
        host='0.0.0.0',
        port=8000,
        reload=False
    )


if __name__ == '__main__':
    main()

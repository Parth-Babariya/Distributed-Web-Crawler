#!/usr/bin/env python3
"""FastAPI search service backed by the MongoDB inverted index."""

import os
import sys
from collections import Counter
from typing import Dict, List

from fastapi import FastAPI, Query
from pymongo import MongoClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import CrawlerConfig
from indexer.mapreduce_indexer import MapReduceIndexer

app = FastAPI(title="Distributed Web Crawler Search API")


class SearchService:
    """Query processor using term-frequency ranking."""

    def __init__(self, mongodb_uri: str = None, database: str = None):
        self.client = MongoClient(mongodb_uri or CrawlerConfig.get_mongo_url())
        self.db = self.client[database or CrawlerConfig.MONGO_DB]
        self.postings = self.db['term_postings']
        self.documents = self.db['pages_documents']
        self.pagerank_nodes = self.db['pagerank_nodes']
        self.indexer = MapReduceIndexer(
            mongodb_uri=mongodb_uri or CrawlerConfig.get_mongo_url(),
            database=database or CrawlerConfig.MONGO_DB
        )

    def search(self, query: str, limit: int = 10) -> Dict:
        terms = self.indexer.tokenize(query)
        if not terms:
            return {'query': query, 'terms': [], 'results': []}

        pipeline = self._build_term_postings_pipeline(terms, limit)
        ranked_docs = list(self.postings.aggregate(
            pipeline,
            allowDiskUse=True,
        ))
        document_text = self._load_document_texts([item['url'] for item in ranked_docs])

        results = []
        for item in ranked_docs:
            snippet = self._make_snippet(document_text.get(item['url'], ''), terms)
            results.append({
                'url': item['url'],
                'title': item.get('title', ''),
                'score': item.get('score', 0.0),
                'term_score': item.get('term_score', 0),
                'authority_score': item.get('authority_score', 1.0),
                'snippet': snippet
            })

        return {
            'query': query,
            'terms': terms,
            'results': results
        }

    def _weighted_term_stages(self, terms: List[str]) -> List[Dict]:
        term_weights = Counter(terms)
        weighted_terms = sorted(term_weights.keys())
        branches = [
            {'case': {'$eq': ['$term', term]}, 'then': weight}
            for term, weight in term_weights.items()
        ]

        return [
            {'$match': {'term': {'$in': weighted_terms}}},
            {
                '$addFields': {
                    'query_weight': {
                        '$switch': {
                            'branches': branches,
                            'default': 1,
                        }
                    }
                }
            },
        ]

    def _authority_score_stages(self, limit: int) -> List[Dict]:
        return [
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
                        '$ifNull': [{'$arrayElemAt': ['$authority.rank', 0]}, 1.0]
                    }
                }
            },
            {
                '$addFields': {
                    'authority_score': {
                        '$cond': [
                            {'$lt': ['$authority_score', 0.0]},
                            0.0,
                            '$authority_score',
                        ]
                    }
                }
            },
            {
                '$addFields': {
                    'score': {'$multiply': ['$term_score', '$authority_score']}
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
                }
            },
        ]

    def _build_term_postings_pipeline(self, terms: List[str], limit: int) -> List[Dict]:
        return [
            *self._weighted_term_stages(terms),
            {
                '$group': {
                    '_id': '$url',
                    'title': {'$first': '$title'},
                    'page_id': {'$first': '$page_id'},
                    'term_score': {
                        '$sum': {
                            '$multiply': [
                                {'$ifNull': ['$term_frequency', 0]},
                                '$query_weight',
                            ]
                        }
                    },
                }
            },
            *self._authority_score_stages(limit),
        ]

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
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    return get_search_service().search(q, limit=limit)


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

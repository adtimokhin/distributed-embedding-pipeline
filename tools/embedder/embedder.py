#!/usr/bin/env python3
"""
Embedding subprocess — line-delimited JSON over stdin/stdout.

Protocol:
  stdin  (one line):  {"chunk_id": "...", "text": "..."}
  stdout (one line):  {"chunk_id": "...", "vector": [...]}

Uses sentence-transformers all-MiniLM-L6-v2 (384-dim).
The model is loaded once at startup and reused for every request.
"""

import json
import sys

from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"


def main():
    model = SentenceTransformer(MODEL_NAME)
    sys.stderr.write(f"[embedder] model {MODEL_NAME} loaded\n")
    sys.stderr.flush()

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            req = json.loads(raw_line)
        except json.JSONDecodeError as e:
            sys.stderr.write(f"[embedder] bad JSON: {e}\n")
            sys.stderr.flush()
            continue

        chunk_id = req.get("chunk_id", "")
        text = req.get("text", "")

        vector = model.encode(text, normalize_embeddings=True).tolist()

        resp = {"chunk_id": chunk_id, "vector": vector}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()

-- Create HNSW vector index on embedding column for semantic search
CREATE INDEX IF NOT EXISTS buyer_quarter_vectors_embedding_idx
ON buyer_quarter_vectors
USING hnsw (embedding vector_cosine_ops);

-- Optionally refresh planner statistics
ANALYZE buyer_quarter_vectors;


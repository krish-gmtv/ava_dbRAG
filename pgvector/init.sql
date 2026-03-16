-- Initialize ava_vectors database with pgvector and core table

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Core table for buyer-quarter vector documents (without embedding column yet)
CREATE TABLE IF NOT EXISTS buyer_quarter_vectors (
    doc_id         TEXT PRIMARY KEY,
    summary_level  TEXT NOT NULL,
    buyer_id       INTEGER NOT NULL,
    buyer_name     TEXT NOT NULL,
    period_year    INTEGER NOT NULL,
    period_quarter INTEGER NOT NULL,
    period_start   DATE,
    period_end     DATE,
    period_label   TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supporting btree indexes for common filters
CREATE INDEX IF NOT EXISTS idx_buyer_quarter_vectors_buyer_id
    ON buyer_quarter_vectors (buyer_id);

CREATE INDEX IF NOT EXISTS idx_buyer_quarter_vectors_period_year
    ON buyer_quarter_vectors (period_year);

CREATE INDEX IF NOT EXISTS idx_buyer_quarter_vectors_buyer_period
    ON buyer_quarter_vectors (buyer_id, period_year, period_quarter);


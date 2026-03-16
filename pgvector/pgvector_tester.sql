SELECT pg_available_extensions.name
FROM pg_available_extensions
WHERE name = 'vector';

select * from buyer_quarter_vectors;

ALTER TABLE buyer_quarter_vectors
ADD COLUMN embedding vector(3072);


SELECT doc_id, buyer_name, period_label, embedding IS NOT NULL
FROM buyer_quarter_vectors;

select * from buyer_quarter_vectors
ORDER BY buyer_name
LIMIT 10;

SELECT doc_id, buyer_name, period_label, embedding IS NOT NULL AS has_embedding
FROM buyer_quarter_vectors
ORDER BY created_at DESC
LIMIT 10;

SELECT
doc_id,
vector_norm(embedding)
FROM buyer_quarter_vectors
LIMIT 10;

SELECT
a.doc_id,
b.doc_id,
1 - (a.embedding <=> b.embedding) AS similarity
FROM buyer_quarter_vectors a
JOIN buyer_quarter_vectors b
ON a.doc_id <> b.doc_id
LIMIT 5;


SELECT payload->'embedding_text'
FROM buyer_quarter_vectors
LIMIT 1;


-- Get its own embedding as a parameter
WITH base AS (
  SELECT embedding
  FROM buyer_quarter_vectors
  WHERE doc_id = 'buyer_3_2018_q1'
)
SELECT
  v.doc_id,
  v.buyer_name,
  v.period_label,
  1 - (v.embedding <=> base.embedding) AS similarity
FROM buyer_quarter_vectors v, base
ORDER BY v.embedding <=> base.embedding
LIMIT 10;


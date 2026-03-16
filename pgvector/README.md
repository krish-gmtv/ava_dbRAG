## Ava pgvector sandbox (semantic retrieval)

This directory defines a **separate** local PostgreSQL instance dedicated to semantic retrieval using `pgvector`. It is completely isolated from your existing Postgres setup and runs on a different port.

- **Container name**: `ava-pgvector`
- **Image**: `pgvector/pgvector:pg16`
- **Host port**: `5433` (container `5432`)
- **Database**: `ava_vectors`
- **Username**: `postgres`
- **Password**: `postgres`

The database is initialized with:

- `pgvector` extension enabled
- `buyer_quarter_vectors` table (without embedding column yet)

---

### 1. Starting the pgvector Postgres instance

From the `pgvector` directory:

```bash
cd "c:\Users\suhru\Desktop\Ava_DB RAG\pgvector"

docker compose up -d
```

Notes:

- This uses port **5433** on your host, so it does **not** conflict with any existing Postgres on port 5432.
- Data is persisted in a named volume: `ava_pgvector_data`.
- `init.sql` is mounted into the container at `/docker-entrypoint-initdb.d/init.sql` and runs automatically on **first** startup of the volume.

To stop the instance:

```bash
docker compose down
```

To stop it but keep the data volume:

```bash
docker compose down --no-remove-orphans
```

(You can always re-run `docker compose up -d` later and it will reuse the same data volume.)

---

### 2. Connecting from pgAdmin

In pgAdmin, create a new server connection:

- **Name**: `ava-pgvector` (any label you like)
- **Host**: `localhost`
- **Port**: `5433`
- **Maintenance DB**: `postgres` (or `ava_vectors` once created)
- **Username**: `postgres`
- **Password**: `postgres`

After connecting, you should see:

- Server: `ava-pgvector`
- Databases: including `ava_vectors`

You can then open a query tool against `ava_vectors` to inspect tables and run test queries.

---

### 3. Verifying pgvector is enabled

From pgAdmin (connected to `ava_vectors`) or from `psql` inside the container:

```sql
SELECT extname
FROM pg_extension
WHERE extname = 'vector';
```

You should get a row:

```text
 extname
---------
 vector
```

You can also use `\dx` in `psql` to list installed extensions.

To quickly inspect the `buyer_quarter_vectors` table:

```sql
\d buyer_quarter_vectors;
```

You should see columns:

- `doc_id` (text, primary key)
- `summary_level` (text, not null)
- `buyer_id` (integer, not null)
- `buyer_name` (text, not null)
- `period_year` (integer, not null)
- `period_quarter` (integer, not null)
- `period_start` (date)
- `period_end` (date)
- `period_label` (text, not null)
- `payload` (jsonb, not null)
- `created_at` (timestamptz, default now())

and indexes on:

- `buyer_id`
- `period_year`
- `(buyer_id, period_year, period_quarter)`

---

### 4. Adding the embedding column later

Once you decide on the embedding dimension (for example, 768 or 1536), you can alter the table to add a `vector` column.

Example for a 768‑dimensional embedding:

```sql
ALTER TABLE buyer_quarter_vectors
ADD COLUMN embedding vector(768);
```

For a 1536‑dimensional embedding:

```sql
ALTER TABLE buyer_quarter_vectors
ADD COLUMN embedding vector(1536);
```

After adding the embedding column, you can also create an **index** to accelerate similarity search:

```sql
-- Example using ivfflat with cosine distance
CREATE INDEX idx_buyer_quarter_vectors_embedding_ivfflat
ON buyer_quarter_vectors
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
```

Notes:

- The `WITH (lists = 100)` parameter can be tuned based on data size.
- You must populate the `embedding` column **before** creating the ivfflat index for best results.

---

### 5. Typical workflow for this pgvector database

1. **Start the container**:

   ```bash
   cd "c:\Users\suhru\Desktop\Ava_DB RAG\pgvector"
   docker compose up -d
   ```

2. **Generate quarterly vector docs** (from your existing scripts) into a JSONL file.

3. **Write a small ingestion script** that:

   - Reads the JSONL docs.
   - Calls your embedding model to compute embeddings.
   - Inserts rows into `buyer_quarter_vectors`:
     - `doc_id`
     - `summary_level`
     - `buyer_id`
     - `buyer_name`
     - `period_year`
     - `period_quarter`
     - `period_start`
     - `period_end`
     - `period_label`
     - `payload` (entire JSON document as `jsonb`)
     - `embedding` (when column is added)

4. **Test retrieval** by:

   - Computing an embedding for a natural‑language query.
   - Running a similarity search against `buyer_quarter_vectors.embedding`, for example:

   ```sql
   SELECT
       doc_id,
       buyer_id,
       buyer_name,
       period_year,
       period_quarter,
       period_label,
       payload,
       1 - (embedding <=> $1::vector) AS similarity
   FROM buyer_quarter_vectors
   ORDER BY embedding <=> $1::vector
   LIMIT 10;
   ```

   where `$1` is a parameter containing the query embedding vector.

This setup keeps your **KPI/precise** database and your **semantic/vector** database fully separated, while still letting you work with both from pgAdmin and VSCode. 


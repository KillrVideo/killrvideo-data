-- ============================================================================
-- Vector Compatibility Test Schema
-- ============================================================================
-- Purpose: Unified schema for testing vector operations across different
--          dimensions and embedding models in Cassandra 5.0+ and Astra DB
-- Target:  default_keyspace in killrvideo database
-- Syntax:  Astra-compatible (uses CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex')
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CLEANUP COMMANDS (commented out for safety)
-- ----------------------------------------------------------------------------
-- Uncomment to drop tables and start fresh:
-- DROP TABLE IF EXISTS default_keyspace.test_vectors_8d;
-- DROP TABLE IF EXISTS default_keyspace.vectors_384;
-- DROP TABLE IF EXISTS default_keyspace.vectors_768;
-- DROP TABLE IF EXISTS default_keyspace.vectors_1024;

-- ============================================================================
-- TABLE DEFINITIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Small Vector Table (8 dimensions)
-- ----------------------------------------------------------------------------
-- Purpose:     Quick format testing and rapid iteration during development
-- Use Case:    Fast vector search testing without large embedding overhead
-- Dimension:   8D (minimal test vectors)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default_keyspace.test_vectors_8d (
    id uuid PRIMARY KEY,
    name text,
    vec vector<float, 8>
);

-- ----------------------------------------------------------------------------
-- IBM Granite Embedding Model (384 dimensions)
-- ----------------------------------------------------------------------------
-- Purpose:     Test vectors from IBM Granite 30M embedding model
-- Use Case:    Lightweight embeddings for resource-constrained environments
-- Model:       IBM Granite 30M (granite-embedding-30m-english)
-- Dimension:   384D
-- Note:        This is the current production dimension used in KillrVideo
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default_keyspace.vectors_384 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 384>
);

-- ----------------------------------------------------------------------------
-- Sentence-BERT all-mpnet-base-v2 (768 dimensions)
-- ----------------------------------------------------------------------------
-- Purpose:     Test vectors from all-mpnet-base-v2 model
-- Use Case:    High-quality sentence embeddings for semantic search
-- Model:       sentence-transformers/all-mpnet-base-v2
-- Dimension:   768D
-- Note:        Popular choice for balanced performance and quality
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default_keyspace.vectors_768 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 768>
);

-- ----------------------------------------------------------------------------
-- BGE Large EN v1.5 (1024 dimensions)
-- ----------------------------------------------------------------------------
-- Purpose:     Test vectors from BAAI BGE large English model
-- Use Case:    State-of-the-art semantic search and retrieval
-- Model:       BAAI/bge-large-en-v1.5
-- Dimension:   1024D
-- Note:        Highest quality embeddings but larger storage footprint
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default_keyspace.vectors_1024 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 1024>
);

-- ============================================================================
-- STORAGE-ATTACHED INDEXES (SAI)
-- ============================================================================
-- SAI enables efficient vector similarity search using COSINE distance
-- Syntax: Astra-compatible (CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex')
-- ============================================================================

-- Index for 8D test vectors
CREATE CUSTOM INDEX IF NOT EXISTS test_vectors_8d_idx
ON default_keyspace.test_vectors_8d(vec)
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- Index for 384D vectors (IBM Granite)
CREATE CUSTOM INDEX IF NOT EXISTS vectors_384_idx
ON default_keyspace.vectors_384(embedding)
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- Index for 768D vectors (all-mpnet-base-v2)
CREATE CUSTOM INDEX IF NOT EXISTS vectors_768_idx
ON default_keyspace.vectors_768(embedding)
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- Index for 1024D vectors (BGE Large)
CREATE CUSTOM INDEX IF NOT EXISTS vectors_1024_idx
ON default_keyspace.vectors_1024(embedding)
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- ============================================================================
-- USAGE NOTES
-- ============================================================================
--
-- Loading this schema:
--   cqlsh -f schema.cql
--
-- Testing vector search (example for 384D table):
--   SELECT id, name, similarity_cosine(embedding, [0.1, 0.2, ...]) AS score
--   FROM default_keyspace.vectors_384
--   ORDER BY embedding ANN OF [0.1, 0.2, ...]
--   LIMIT 10;
--
-- Important considerations:
--   - Vector dimensions must match EXACTLY when querying
--   - Ensure sufficient heap memory for large vector operations
--   - SAI indexes are built asynchronously; check index status before querying
--   - COSINE similarity returns values in range [-1, 1] where 1 is most similar
--
-- ============================================================================

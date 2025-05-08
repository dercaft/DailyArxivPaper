-- PostgreSQL schema for Daily Arxiv Paper Analysis Platform

-- Enable pgvector extension if not already enabled
-- You need to have the pgvector extension installed in your PostgreSQL server.
CREATE EXTENSION IF NOT EXISTS vector;

-- Enable pg_trgm extension for fuzzy string matching (optional but recommended for author names)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Table to store metadata for arXiv categories
CREATE TABLE categories_meta (
    category_code VARCHAR(50) PRIMARY KEY, -- e.g., "cs.AI", "eess.AS", "math.CO"
    description TEXT NULL                  -- Optional: A friendly name or description for the category
);

-- Table to store unique author names
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    name VARCHAR(512) UNIQUE NOT NULL      -- Ensuring author names are unique
);

-- Main table for arXiv papers
CREATE TABLE papers (
    -- Core arXiv Information
    id VARCHAR(50) PRIMARY KEY,               -- arXiv ID, e.g., "2401.12345v1" or "abs/2401.12345"
    title TEXT NOT NULL,
    abstract TEXT,
    primary_category_code VARCHAR(50) REFERENCES categories_meta(category_code), -- Primary arXiv category
    pdf_url TEXT,                             -- Link to PDF on arXiv
    arxiv_published_at TIMESTAMP WITH TIME ZONE, -- Date/time paper was first published on arXiv
    arxiv_updated_at TIMESTAMP WITH TIME ZONE,   -- Date/time paper was last updated on arXiv

    -- AI-Generated Content
    summary_ai TEXT,                          -- AI-generated summary
    detailed_review_ai TEXT,                  -- AI-generated detailed review (Markdown format)

    -- PDF Download Tracking
    is_pdf_downloaded BOOLEAN DEFAULT FALSE,
    pdf_storage_path TEXT NULL,               -- Path/key in cloud storage or local filesystem
    pdf_downloaded_at TIMESTAMP WITH TIME ZONE NULL,

    -- Formal Publication Details
    journal_ref TEXT NULL,                    -- Journal reference string from arXiv (e.g., "Nature Physics 12, 345-350 (2023)")
    doi VARCHAR(255) NULL,                    -- Digital Object Identifier (e.g., "10.1038/s41567-023-01234-5")

    -- For Full-Text Search (FTS)
    -- This column will be populated by a trigger or application logic
    fts_document TSVECTOR,

    -- For Semantic Search (using pgvector extension)
    -- Replace 1024 with the actual dimension of your embeddings
    title_abstract_embedding VECTOR(1024),    -- Combined embedding for title and abstract
    summary_review_embedding VECTOR(1024)     -- Combined embedding for AI summary and AI detailed review
);

-- Junction table for many-to-many relationship between papers and authors
CREATE TABLE paper_authors (
    paper_id VARCHAR(50) REFERENCES papers(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
    author_order INTEGER NULL, -- Optional: to maintain the order of authors as they appear on the paper
    PRIMARY KEY (paper_id, author_id)
);

-- Junction table for many-to-many relationship between papers and all their categories
CREATE TABLE paper_categories (
    paper_id VARCHAR(50) REFERENCES papers(id) ON DELETE CASCADE,
    category_code VARCHAR(50) REFERENCES categories_meta(category_code) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, category_code)
);

--------------------------------------------------------------------------------
-- INDEXES FOR PERFORMANCE
--------------------------------------------------------------------------------

-- For Full-Text Search on the 'papers' table
CREATE INDEX idx_papers_fts_document ON papers USING GIN(fts_document);

-- For Semantic Search on the 'papers' table using HNSW (Hierarchical Navigable Small World)
-- Choose the appropriate operator class based on your distance metric:
-- vector_l2_ops: Euclidean distance
-- vector_ip_ops: Inner product (maximize for cosine similarity if vectors are normalized)
-- vector_cosine_ops: Cosine distance (minimize for cosine similarity)
-- For sentence embeddings, cosine similarity (thus vector_cosine_ops for distance) is common.
-- The HNSW index parameters (m, ef_construction) can be tuned for your dataset and performance needs.

CREATE INDEX idx_papers_title_abstract_embedding ON papers USING HNSW (title_abstract_embedding vector_cosine_ops);
CREATE INDEX idx_papers_summary_review_embedding ON papers USING HNSW (summary_review_embedding vector_cosine_ops);

-- Indexes for faster lookups and filtering on common fields
CREATE INDEX idx_papers_arxiv_published_at ON papers(arxiv_published_at DESC);
CREATE INDEX idx_papers_arxiv_updated_at ON papers(arxiv_updated_at DESC);
CREATE INDEX idx_papers_primary_category_code ON papers(primary_category_code);
CREATE INDEX idx_papers_doi ON papers(doi) WHERE doi IS NOT NULL; -- Index only non-NULL DOIs

-- Indexes for PDF download status
CREATE INDEX idx_papers_is_pdf_downloaded ON papers(is_pdf_downloaded) WHERE is_pdf_downloaded = TRUE; -- Consider if filtering by FALSE is also common

-- Indexes for junction tables to speed up joins
CREATE INDEX idx_paper_authors_paper_id ON paper_authors(paper_id);
CREATE INDEX idx_paper_authors_author_id ON paper_authors(author_id);

CREATE INDEX idx_paper_categories_paper_id ON paper_categories(paper_id);
CREATE INDEX idx_paper_categories_category_code ON paper_categories(category_code);

-- Index on author name for quick searching/linking
CREATE INDEX idx_authors_name_trgm ON authors USING GIN (name gin_trgm_ops); -- For faster LIKE/ILIKE searches (requires pg_trgm extension)
-- CREATE INDEX idx_authors_name ON authors(name); -- Standard B-tree index (alternative if not using pg_trgm or for exact matches)

-- Index on category_code in categories_meta (already primary key, but good for clarity)
-- The PRIMARY KEY constraint automatically creates a UNIQUE B-tree index.

--------------------------------------------------------------------------------
-- OPTIONAL: TRIGGER FUNCTION TO POPULATE fts_document
--------------------------------------------------------------------------------
-- This trigger automatically updates the 'fts_document' column whenever
-- relevant text fields in the 'papers' table are inserted or updated.
-- You'll need to adapt the fields and weights based on your priorities.

CREATE OR REPLACE FUNCTION update_papers_fts_document()
RETURNS TRIGGER AS $$
DECLARE
    paper_authors_text TEXT;
BEGIN
    -- Aggregate author names for the current paper
    SELECT string_agg(authors.name, ' ')
    INTO paper_authors_text
    FROM paper_authors
    JOIN authors ON paper_authors.author_id = authors.author_id
    WHERE paper_authors.paper_id = NEW.id;

    -- Update the fts_document with weighted contributions
    -- 'english' is the text search configuration; change if needed for other languages.
    NEW.fts_document :=
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.title,'')), 'A') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.abstract,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(paper_authors_text,'')), 'C') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.summary_ai,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.detailed_review_ai,'')), 'D'); -- Lower weight for detailed review
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER papers_fts_update_trigger
BEFORE INSERT OR UPDATE ON papers
FOR EACH ROW EXECUTE FUNCTION update_papers_fts_document();

--------------------------------------------------------------------------------
-- EXAMPLE: Populating categories_meta with some common CS categories
--------------------------------------------------------------------------------
-- You would populate this table with all relevant arXiv categories you intend to handle.
INSERT INTO categories_meta (category_code, description) VALUES
('cs.AI', 'Computer Science - Artificial Intelligence'),
('cs.CL', 'Computer Science - Computation and Language'),
('cs.CV', 'Computer Science - Computer Vision and Pattern Recognition'),
('cs.LG', 'Computer Science - Machine Learning'),
('cs.RO', 'Computer Science - Robotics'),
('stat.ML', 'Statistics - Machine Learning')
ON CONFLICT (category_code) DO NOTHING;

COMMIT;

-- Notes:
-- 1. Ensure the pgvector and pg_trgm extensions are installed in your PostgreSQL instance before running this.
-- 2. The embedding dimension `VECTOR(1024)` is an example. Adjust it based on your chosen embedding model. Both embedding columns should ideally use the same dimension if generated by the same model.
-- 3. The HNSW index parameters (`m`, `ef_construction`) and FTS weights in the trigger are examples and can be tuned for performance and relevance.
-- 4. The `author_order` in `paper_authors` is optional but useful if you need to preserve author sequence.
-- 5. The FTS trigger `update_papers_fts_document` remains unchanged as it populates the tsvector based on text fields and is independent of the embedding strategy.
-- 6. Storing combined embeddings like `summary_review_embedding` (which might include the full `detailed_review_ai`) could lead to very large vector inputs if the combined text is very long before embedding. Consider the implications for your embedding model and generation process.
-- 7. You are now using combined embeddings (`title_abstract_embedding`, `summary_review_embedding`). This strategy is good for searching across these combined concepts. Ensure your embedding generation process correctly combines the respective text fields before creating the vector.

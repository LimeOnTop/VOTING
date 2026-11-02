CREATE TABLE IF NOT EXISTS polls (
 id text PRIMARY KEY, question text NOT NULL, type text NOT NULL CHECK(type IN ('single','multiple')),
 options jsonb NOT NULL, opens_at timestamptz NOT NULL, closes_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL, CHECK(closes_at > opens_at)
);
-- Minimal deduplication ledger: no individual selections, raw cookie or IP.
CREATE TABLE IF NOT EXISTS vote_dedup (
 poll_id text NOT NULL REFERENCES polls(id), voter_key text NOT NULL,
 PRIMARY KEY(poll_id,voter_key)
);
-- Avoid a single counter row for a hot poll; production needs independent database shards too.
CREATE TABLE IF NOT EXISTS poll_counts (
 poll_id text NOT NULL REFERENCES polls(id), shard smallint NOT NULL,
 option_id text NOT NULL, votes bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(poll_id,shard,option_id)
);
CREATE TABLE IF NOT EXISTS poll_progress (
 poll_id text NOT NULL REFERENCES polls(id), shard smallint NOT NULL,
 voters bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL,
 PRIMARY KEY(poll_id,shard)
);

CREATE TABLE IF NOT EXISTS projection_topology (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), fingerprint text NOT NULL
);
CREATE TABLE IF NOT EXISTS finalized_polls (
 poll_id text PRIMARY KEY REFERENCES polls(id), finalized_at timestamptz NOT NULL DEFAULT now()
);

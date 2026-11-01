CREATE TABLE IF NOT EXISTS polls (
 id text PRIMARY KEY, question text NOT NULL, type text NOT NULL CHECK(type IN ('single','multiple')),
 options jsonb NOT NULL, opens_at timestamptz NOT NULL, closes_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL, CHECK(closes_at > opens_at)
);
-- Poll-specific pseudonyms only; no IP, raw cookie or user-agent is persisted.
CREATE TABLE IF NOT EXISTS votes (
 poll_id text NOT NULL REFERENCES polls(id), voter_key text NOT NULL,
 options jsonb NOT NULL, accepted_at timestamptz NOT NULL,
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

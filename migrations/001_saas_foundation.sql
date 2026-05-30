-- ============================================================
-- Drev CI: SaaS Foundation Schema
-- Run this in Supabase SQL Editor (Settings > SQL Editor)
-- Project: okgoarstmlrxcwtfotbp.supabase.co
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Organizations (each paying customer = one org)
-- ============================================================
CREATE TABLE organizations (
  id                 UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  name               TEXT        NOT NULL,
  slug               TEXT        NOT NULL UNIQUE,
  plan               TEXT        NOT NULL DEFAULT 'free',
                    -- 'free' | 'team' | 'business' | 'enterprise'
  worker_limit       INT         NOT NULL DEFAULT 1,
  queue_limit        INT         NOT NULL DEFAULT 10,
  log_retention_days INT         NOT NULL DEFAULT 7,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Members (users belong to orgs with roles)
-- ============================================================
CREATE TABLE org_members (
  id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id     UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  user_id    UUID        NOT NULL,
             -- references auth.users (Supabase Auth)
  role       TEXT        NOT NULL DEFAULT 'member',
             -- 'owner' | 'admin' | 'member' | 'viewer'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(org_id, user_id)
);

-- ============================================================
-- API Tokens (scoped to org)
-- ============================================================
CREATE TABLE api_tokens (
  id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id       UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name         TEXT        NOT NULL,
  token_hash   TEXT        NOT NULL UNIQUE,
               -- store SHA256 hash, never plaintext
  last_used_at TIMESTAMPTZ,
  created_by   UUID        NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at   TIMESTAMPTZ
               -- NULL = never expires
);

-- ============================================================
-- Pipeline configs (scoped to org)
-- ============================================================
CREATE TABLE pipelines (
  id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id     UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name       TEXT        NOT NULL,
  config     JSONB       NOT NULL,
             -- parsed .drev.yml stored as JSON
  repo_url   TEXT,
  ref        TEXT        DEFAULT 'main',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(org_id, name)
);

-- ============================================================
-- Runs (scoped to org + pipeline)
-- ============================================================
CREATE TABLE runs (
  id            UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id        UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  pipeline_id   UUID        REFERENCES pipelines(id) ON DELETE SET NULL,
  pipeline_name TEXT        NOT NULL,
  status        TEXT        NOT NULL DEFAULT 'pending',
                -- pending|running|success|failed|cancelled
  triggered_by  TEXT        NOT NULL DEFAULT 'api',
                -- 'api' | 'webhook' | 'schedule'
  commit_sha    TEXT,
  commit_msg    TEXT,
  branch        TEXT,
  started_at    TIMESTAMPTZ,
  finished_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Run Jobs
-- ============================================================
CREATE TABLE run_jobs (
  id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id      UUID        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  job_name    TEXT        NOT NULL,
  status      TEXT        NOT NULL DEFAULT 'pending',
  started_at  TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

-- ============================================================
-- Webhook configs (per repo per org)
-- ============================================================
CREATE TABLE webhook_configs (
  id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id         UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  repo_full_name TEXT        NOT NULL,
                 -- e.g. "drevci/drev"
  pipeline_id    UUID        NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  secret_hash    TEXT        NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(org_id, repo_full_name)
);

-- ============================================================
-- Usage tracking (for billing metering)
-- ============================================================
CREATE TABLE usage_events (
  id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  org_id      UUID        NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  event_type  TEXT        NOT NULL,
              -- 'pipeline_run' | 'worker_minute'
  run_id      UUID        REFERENCES runs(id),
  quantity    NUMERIC     NOT NULL DEFAULT 1,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Indexes for performance
-- ============================================================
CREATE INDEX idx_runs_org_id     ON runs(org_id);
CREATE INDEX idx_runs_status     ON runs(status);
CREATE INDEX idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX idx_run_jobs_run_id ON run_jobs(run_id);
CREATE INDEX idx_usage_org_id    ON usage_events(org_id);
CREATE INDEX idx_usage_recorded_at ON usage_events(recorded_at DESC);
CREATE INDEX idx_api_tokens_hash ON api_tokens(token_hash);
CREATE INDEX idx_org_members_user ON org_members(user_id);

-- ============================================================
-- Row Level Security (RLS)
-- ============================================================
ALTER TABLE organizations   ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_members     ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_tokens      ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipelines       ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs            ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_jobs        ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_events    ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- RLS Policies: users only see their org's data
-- ============================================================

CREATE POLICY "org_members_see_own_org" ON organizations
  FOR ALL USING (
    id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_members" ON org_members
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_tokens" ON api_tokens
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_pipelines" ON pipelines
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_runs" ON runs
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_jobs" ON run_jobs
  FOR ALL USING (
    run_id IN (
      SELECT id FROM runs WHERE org_id IN (
        SELECT org_id FROM org_members
        WHERE user_id = auth.uid()
      )
    )
  );

CREATE POLICY "members_see_own_org_webhooks" ON webhook_configs
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "members_see_own_org_usage" ON usage_events
  FOR ALL USING (
    org_id IN (
      SELECT org_id FROM org_members
      WHERE user_id = auth.uid()
    )
  );

-- ============================================================
-- Service role bypass (backend uses service_role key — bypasses RLS)
-- No additional policy needed; service_role always bypasses RLS.
-- ============================================================

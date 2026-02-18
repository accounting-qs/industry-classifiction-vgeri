-- Create a table for structured persistent pipeline logs
CREATE TABLE IF NOT EXISTS pipeline_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  timestamp TIMESTAMP WITH TIME ZONE DEFAULT now(),
  instance_id TEXT,
  module TEXT,
  message TEXT,
  level TEXT DEFAULT 'info',
  metadata JSONB DEFAULT '{}'
);

-- Index for faster retrieval of latest logs
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_timestamp ON pipeline_logs(timestamp DESC);

-- Create scraped_data table for master cache of website content
CREATE TABLE IF NOT EXISTS public.scraped_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT UNIQUE NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Enable RLS
ALTER TABLE public.scraped_data ENABLE ROW LEVEL SECURITY;

-- Create policy for public access if needed (optional, adjust based on security needs)
CREATE POLICY "Allow service role full access" ON public.scraped_data
    USING (true)
    WITH CHECK (true);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_scraped_data_domain ON public.scraped_data(domain);

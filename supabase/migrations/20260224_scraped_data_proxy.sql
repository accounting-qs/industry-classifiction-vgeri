-- Add proxy_used column to track which proxy succeeded
ALTER TABLE public.scraped_data
ADD COLUMN IF NOT EXISTS proxy_used TEXT;

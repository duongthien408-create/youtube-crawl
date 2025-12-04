# YouTube Video Fetcher

Fetch YouTube videos with transcripts and save to Supabase database.

## Features

- Fetch videos from multiple YouTube channels
- Extract transcripts (Vietnamese or English)
- Save to Supabase database
- Skip YouTube Shorts (optional)
- Run manually or via GitHub Actions cron job

## Setup

### 1. Clone repository

```bash
git clone https://github.com/your-username/youtube-fetcher.git
cd youtube-fetcher
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

Copy `.env.example` to `.env` and fill in your Supabase credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-key
```

### 4. Database schema

Create these tables in Supabase:

```sql
-- Creators table
CREATE TABLE creators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    channel_url TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Posts table
CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID REFERENCES creators(id),
    type TEXT DEFAULT 'review',
    status TEXT DEFAULT 'draft',
    language TEXT DEFAULT 'vi',
    source TEXT DEFAULT 'youtube',
    platform TEXT DEFAULT 'youtube',
    title TEXT,
    summary TEXT,
    source_url TEXT UNIQUE,
    thumbnail_url TEXT,
    transcript TEXT,
    transcript_en TEXT,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Usage

### Run locally

```bash
# Fetch all channels (Vietnamese + English)
python scripts/fetch_youtube.py

# Fetch only Vietnamese channels
python scripts/fetch_youtube.py --lang vi

# Fetch only English channels
python scripts/fetch_youtube.py --lang en

# Fetch 10 videos per channel
python scripts/fetch_youtube.py --limit 10

# Include YouTube Shorts
python scripts/fetch_youtube.py --include-shorts
```

### GitHub Actions

The workflow runs automatically every day at 8:00 AM Vietnam time.

To run manually:
1. Go to Actions tab in your repository
2. Select "Fetch YouTube Videos"
3. Click "Run workflow"
4. Choose language and limit

### GitHub Secrets

Add these secrets to your repository:
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase anon or service key

## Configure Channels

Edit `scripts/fetch_youtube.py` to add/remove channels:

```python
CHANNELS_VI = [
    {
        "name": "Channel Name",
        "channel_id": "UCxxxxxxxxxxxxxxxx",
    },
]

CHANNELS_EN = [
    {
        "name": "English Channel",
        "channel_id": "UCxxxxxxxxxxxxxxxx",
    },
]
```

To find a channel ID:
1. Go to the YouTube channel page
2. View page source
3. Search for `"channelId"` or `"externalId"`

## License

MIT

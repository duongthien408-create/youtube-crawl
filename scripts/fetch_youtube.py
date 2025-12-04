#!/usr/bin/env python3
"""
YouTube Video + Transcript Fetcher

Fetches videos from YouTube channels and saves to Supabase database.
Supports both Vietnamese and English channels with transcript extraction.

Usage:
    python scripts/fetch_youtube.py
    python scripts/fetch_youtube.py --lang en
    python scripts/fetch_youtube.py --lang vi
    python scripts/fetch_youtube.py --channel UCVDS-cjZZIWPjL5fdXSMywQ --limit 10
"""

import os
import sys
import json
import re
import time
import argparse
import tempfile
import yt_dlp
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ==============================================
# CONFIGURATION - Use environment variables
# ==============================================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_KEY environment variables are required")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================
# CHANNEL CONFIGURATION
# ==============================================

# Vietnamese YouTube channels
CHANNELS_VI = [
    {
        "name": "GEARVN",
        "channel_id": "UCdxRpD_T4-HzPsely-Fcezw",
    },
    {
        "name": "Nguoi Choi Do",
        "channel_id": "UC3HxHh_jezfVCcXNCyDJHOQ",
    },
    {
        "name": "GenZ Viet",
        "channel_id": "UCMSDj69umhJodE1BLJNxYIw",
    },
    {
        "name": "Vinh Xo",
        "channel_id": "UCyqxvGyF5LO67HI6vdE5bfQ",
    },
    {
        "name": "Vat Vo Studio",
        "channel_id": "UCEeXA5Tu7n9X5_zkOgGsyww",
    },
    {
        "name": "Binh Bear",
        "channel_id": "UCTymg6O7vl87L0c5SdZVAeQ",
    },
    {
        "name": "Tai Xai Tech",
        "channel_id": "UCiYYo7oPjA_MQ9i7-zoNfGA",
    },
]

# English YouTube channels
CHANNELS_EN = [
    {
        "name": "Just Josh",
        "channel_id": "UCtHm9ai5zSb-yfRnnUBopAg",
    },
    {
        "name": "Jarrod's Tech",
        "channel_id": "UC2Rzju32yQPkQ7oIhmeuLwg",
    },
    {
        "name": "NoodleNick",
        "channel_id": "UCthAJeiDA_7iKyzYElbrgjg",
    },
]

# ==============================================
# DATABASE OPERATIONS
# ==============================================

def get_or_create_channel(channel_id: str, language: str = 'vi') -> dict:
    """Get or create channel in database, returns channel record with UUID"""
    # Check if channel exists
    response = supabase.table('channels').select('*').eq('channel_id', channel_id).execute()
    if response.data:
        return response.data[0]

    # Fetch channel info from YouTube
    channel_url = f"https://www.youtube.com/channel/{channel_id}"
    ydl_opts = {'quiet': True, 'no_warnings': True}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(channel_url, download=False, process=False)
            channel_name = info.get('channel', info.get('uploader', 'Unknown'))
            description = info.get('description', '')
            thumbnail = info.get('thumbnail', '')
    except:
        channel_name = channel_id
        description = ''
        thumbnail = ''

    slug = channel_name.lower().replace(' ', '-').replace("'", '').replace('.', '')

    # Insert new channel
    channel_data = {
        'channel_id': channel_id,
        'name': channel_name,
        'slug': slug,
        'description': description[:500] if description else None,
        'thumbnail_url': thumbnail,
        'language': language,
        'is_active': True,
    }

    response = supabase.table('channels').insert(channel_data).execute()
    print(f"  Created channel: {channel_name}")
    return response.data[0]


def get_or_create_creator(name: str, channel_url: str) -> str:
    """Get or create creator in database"""
    response = supabase.table('creators').select('id').eq('name', name).execute()
    if response.data:
        return response.data[0]['id']

    slug = name.lower().replace(' ', '-').replace("'", '')
    response = supabase.table('creators').insert({
        'name': name,
        'slug': slug,
        'channel_url': channel_url,
    }).execute()

    print(f"  Created creator: {name}")
    return response.data[0]['id']


def check_video_exists(video_id: str) -> bool:
    """Check if video already exists in database"""
    response = supabase.table('videos').select('id').eq('video_id', video_id).execute()
    return len(response.data) > 0


def save_video_to_database(video: dict, channel_uuid: str, language: str, transcript_data: dict = None):
    """Save video to videos table"""

    # Parse upload date
    published_at = None
    if video.get('upload_date'):
        try:
            published_at = datetime.strptime(video['upload_date'], '%Y%m%d').isoformat()
        except:
            published_at = datetime.now().isoformat()
    else:
        published_at = datetime.now().isoformat()

    video_data = {
        'channel_id': channel_uuid,
        'video_id': video.get('id'),
        'title': video.get('title', ''),
        'description': video.get('description', '')[:2000] if video.get('description') else None,
        'thumbnail_url': video.get('thumbnail'),
        'duration': video.get('duration'),
        'view_count': video.get('view_count'),
        'published_at': published_at,
        'source_url': video.get('url'),
        'transcript_language': language,
        'status': 'draft',
    }

    # Add transcript
    if transcript_data:
        video_data['transcript'] = transcript_data.get('transcript', '')
        video_data['transcript_timestamped'] = transcript_data.get('transcript_timestamped', '')

    try:
        response = supabase.table('videos').insert(video_data).execute()
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"    Error saving to database: {e}")
        return None


def save_to_database(video: dict, creator_id: str, language: str, transcript_data: dict = None):
    """Save video to posts table (legacy support)"""

    # Parse upload date
    published_at = None
    if video.get('upload_date'):
        try:
            published_at = datetime.strptime(video['upload_date'], '%Y%m%d').isoformat()
        except:
            published_at = datetime.now().isoformat()
    else:
        published_at = datetime.now().isoformat()

    post_data = {
        'creator_id': creator_id,
        'type': 'review',
        'status': 'draft',
        'language': language,
        'source': 'youtube',
        'platform': 'youtube',
        'title': video.get('title', ''),
        'summary': video.get('description', '')[:200] if video.get('description') else '',
        'source_url': video.get('url'),
        'thumbnail_url': video.get('thumbnail'),
        'published_at': published_at,
        'created_at': datetime.now().isoformat(),
    }

    # Add transcript based on language
    if transcript_data:
        if language == 'vi':
            post_data['transcript'] = transcript_data.get('transcript', '')
        else:
            post_data['transcript_en'] = transcript_data.get('transcript', '')

    try:
        response = supabase.table('posts').insert(post_data).execute()
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"    Error saving to database: {e}")
        return None


# ==============================================
# YOUTUBE OPERATIONS
# ==============================================

def parse_json3_subtitles(data: dict) -> tuple[str, str]:
    """Parse YouTube json3 subtitle format to plain text and timestamped text"""
    if 'events' not in data:
        return "", ""

    plain_lines = []
    timestamped_lines = []

    for event in data.get('events', []):
        if 'segs' in event:
            start_ms = event.get('tStartMs', 0)
            seconds = start_ms // 1000
            minutes = seconds // 60
            secs = seconds % 60
            timestamp = f'{minutes:02d}:{secs:02d}'

            text = ''.join(seg.get('utf8', '') for seg in event['segs'])
            text = text.strip()
            if text and text != '\n':
                plain_lines.append(text)
                timestamped_lines.append(f'[{timestamp}] {text}')

    return ' '.join(plain_lines), '\n'.join(timestamped_lines)


def fetch_transcript(video_id: str, video_url: str, language: str) -> dict:
    """Fetch transcript using yt-dlp"""
    print(f"    Fetching {language.upper()} transcript...")

    with tempfile.TemporaryDirectory() as temp_dir:
        ydl_opts = {
            'skip_download': True,
            'writesubtitles': True,
            'writeautomaticsub': True,
            'subtitleslangs': [language],
            'subtitlesformat': 'json3',
            'outtmpl': os.path.join(temp_dir, video_id),
            'quiet': True,
            'no_warnings': True,
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(video_url, download=True)

            # Check for subtitle file
            subtitle_file = os.path.join(temp_dir, f'{video_id}.{language}.json3')
            if os.path.exists(subtitle_file):
                with open(subtitle_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                plain_text, timestamped_text = parse_json3_subtitles(data)

                print(f"    Transcript: {len(plain_text)} chars")
                return {
                    'transcript': plain_text,
                    'transcript_timestamped': timestamped_text,
                }

            print(f"    No {language.upper()} subtitles found")
            return None

        except Exception as e:
            print(f"    Error fetching transcript: {e}")
            return None


def fetch_videos_from_channel(channel_url: str, limit: int = 5, skip_shorts: bool = True, offset: int = 0) -> list:
    """Fetch videos from YouTube channel with optional offset for pagination"""
    print(f"Fetching {limit} videos from channel (offset: {offset})...")

    # Calculate playlist_items range for offset support
    start = offset + 1
    end = offset + (limit * 2)  # Fetch more to filter out Shorts

    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'playlist_items': f'{start}-{end}',
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)

            if 'entries' not in info:
                print("No videos found")
                return []

            videos = []
            for entry in info['entries']:
                if entry:
                    video_url = entry.get('url', '')

                    # Skip Shorts if requested
                    if skip_shorts and '/shorts/' in video_url:
                        continue

                    videos.append({
                        'id': entry.get('id'),
                        'title': entry.get('title'),
                        'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                    })

                    if len(videos) >= limit:
                        break

            print(f"Found {len(videos)} videos")
            return videos

    except Exception as e:
        print(f"Error fetching channel: {e}")
        return []


def get_video_details(video_url: str, skip_shorts: bool = True) -> dict:
    """Get full video details"""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)

            # Check if it's a Short (duration < 60s)
            duration = info.get('duration', 0)
            if skip_shorts and duration and duration < 60:
                return None  # Skip Shorts

            return {
                'id': info.get('id'),
                'title': info.get('title'),
                'description': info.get('description', '')[:500],
                'thumbnail': info.get('thumbnail'),
                'duration': duration,
                'upload_date': info.get('upload_date'),
                'view_count': info.get('view_count'),
            }
    except Exception as e:
        print(f"Error getting video details: {e}")
        return None


# ==============================================
# MAIN PROCESSING
# ==============================================

def process_channel(channel_config: dict, language: str, limit: int = 5, skip_shorts: bool = True) -> tuple[int, int]:
    """Process a single channel and return (success_count, skip_count)"""
    channel_id = channel_config['channel_id']
    channel_name = channel_config['name']
    channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"

    print(f"\n{'='*60}")
    print(f"Processing: {channel_name}")
    print(f"Channel ID: {channel_id}")
    print(f"Language: {language.upper()}")

    # Get or create creator
    creator_id = get_or_create_creator(channel_name, channel_url)
    print(f"Creator ID: {creator_id}")

    # Fetch videos
    videos = fetch_videos_from_channel(channel_url, limit=limit, skip_shorts=skip_shorts)

    if not videos:
        print("No videos found")
        return 0, 0

    success_count = 0
    skip_count = 0

    for i, video in enumerate(videos, 1):
        title_preview = video['title'][:50] if video.get('title') else 'No title'
        print(f"\n[{i}/{len(videos)}] {title_preview}...")

        # Check if already exists
        if check_video_exists(video['url']):
            print(f"    Already exists, skipping...")
            skip_count += 1
            continue

        # Get full video details
        print(f"    Getting video details...")
        details = get_video_details(video['url'], skip_shorts=skip_shorts)
        if not details:
            print(f"    Skipping (possibly a Short)")
            skip_count += 1
            continue
        video.update(details)

        # Fetch transcript
        transcript_data = fetch_transcript(video['id'], video['url'], language)

        # Save to database
        print(f"    Saving to database...")
        result = save_to_database(video, creator_id, language, transcript_data)

        if result:
            print(f"    Saved! Post ID: {result['id']}")
            success_count += 1
        else:
            print(f"    Failed to save")

        # Rate limiting
        time.sleep(2)

    return success_count, skip_count


def process_single_channel(channel_id: str, language: str, limit: int = 5, skip_shorts: bool = True, offset: int = 0, delay: int = 5, min_views: int = 0) -> tuple[int, int]:
    """Process a single channel by ID using new schema (channels + videos tables)"""
    channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"

    print(f"\n{'='*60}")
    print(f"Processing channel: {channel_id}")
    print(f"Language: {language.upper()}")
    print(f"Offset: {offset}, Limit: {limit}, Delay: {delay}s")
    if min_views > 0:
        print(f"Min views filter: {min_views:,}")

    # Get or create channel in database
    channel_record = get_or_create_channel(channel_id, language)
    channel_uuid = channel_record['id']
    channel_name = channel_record['name']

    print(f"Channel: {channel_name}")
    print(f"Channel UUID: {channel_uuid}")

    # Fetch videos
    videos = fetch_videos_from_channel(channel_url, limit=limit, skip_shorts=skip_shorts, offset=offset)

    if not videos:
        print("No videos found")
        return 0, 0

    success_count = 0
    skip_count = 0

    for i, video in enumerate(videos, 1):
        title_preview = video['title'][:50] if video.get('title') else 'No title'
        print(f"\n[{i}/{len(videos)}] {title_preview}...")

        # Check if already exists
        if check_video_exists(video['id']):
            print(f"    Already exists, skipping...")
            skip_count += 1
            continue

        # Get full video details
        print(f"    Getting video details...")
        details = get_video_details(video['url'], skip_shorts=skip_shorts)
        if not details:
            print(f"    Skipping (possibly a Short)")
            skip_count += 1
            continue
        video.update(details)

        # Check minimum views filter
        view_count = video.get('view_count', 0) or 0
        if min_views > 0 and view_count < min_views:
            print(f"    Skipping (only {view_count:,} views, need {min_views:,})")
            skip_count += 1
            continue

        print(f"    Views: {view_count:,}")

        # Fetch transcript
        transcript_data = fetch_transcript(video['id'], video['url'], language)

        # Save to videos table
        print(f"    Saving to database...")
        result = save_video_to_database(video, channel_uuid, language, transcript_data)

        if result:
            print(f"    Saved! Video ID: {result['id']}")
            success_count += 1
        else:
            print(f"    Failed to save")

        # Rate limiting - configurable delay
        print(f"    Waiting {delay}s...")
        time.sleep(delay)

    return success_count, skip_count


def main():
    parser = argparse.ArgumentParser(description='Fetch YouTube videos with transcripts')
    parser.add_argument('--lang', choices=['vi', 'en', 'all'], default='all',
                        help='Language to fetch (vi, en, or all)')
    parser.add_argument('--limit', type=int, default=5,
                        help='Number of videos per channel')
    parser.add_argument('--offset', type=int, default=0,
                        help='Offset for pagination (skip first N videos)')
    parser.add_argument('--delay', type=int, default=5,
                        help='Delay between videos in seconds (default: 5)')
    parser.add_argument('--include-shorts', action='store_true',
                        help='Include YouTube Shorts')
    parser.add_argument('--channel', type=str, default=None,
                        help='Single channel ID to fetch (e.g., UCVDS-cjZZIWPjL5fdXSMywQ)')
    parser.add_argument('--min-views', type=int, default=0,
                        help='Minimum view count to include video (default: 0 = no filter)')
    args = parser.parse_args()

    print("=" * 60)
    print("YouTube Video + Transcript Fetcher")
    print(f"Language: {args.lang.upper()}")
    print(f"Limit: {args.limit} videos per channel")
    print(f"Offset: {args.offset}")
    print(f"Delay: {args.delay}s between videos")
    print(f"Skip Shorts: {not args.include_shorts}")
    if args.min_views > 0:
        print(f"Min views: {args.min_views:,}")
    print("=" * 60)

    total_success = 0
    total_skip = 0

    # Single channel mode
    if args.channel:
        print(f"\n>>> SINGLE CHANNEL MODE <<<")
        lang = args.lang if args.lang != 'all' else 'vi'
        success, skip = process_single_channel(
            args.channel,
            language=lang,
            limit=args.limit,
            skip_shorts=not args.include_shorts,
            offset=args.offset,
            delay=args.delay,
            min_views=args.min_views
        )
        total_success += success
        total_skip += skip
    else:
        # Process Vietnamese channels
        if args.lang in ['vi', 'all']:
            print("\n>>> VIETNAMESE CHANNELS <<<")
            for channel in CHANNELS_VI:
                success, skip = process_channel(
                    channel,
                    language='vi',
                    limit=args.limit,
                    skip_shorts=not args.include_shorts
                )
                total_success += success
                total_skip += skip

        # Process English channels
        if args.lang in ['en', 'all']:
            print("\n>>> ENGLISH CHANNELS <<<")
            for channel in CHANNELS_EN:
                success, skip = process_channel(
                    channel,
                    language='en',
                    limit=args.limit,
                    skip_shorts=not args.include_shorts
                )
                total_success += success
                total_skip += skip

    print("\n" + "=" * 60)
    print(f"ALL DONE!")
    print(f"  Total saved: {total_success}")
    print(f"  Total skipped: {total_skip}")
    print("=" * 60)


if __name__ == '__main__':
    main()

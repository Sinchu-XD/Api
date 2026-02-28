import asyncio
import os
import uuid
from mongo import fs, get_existing_file
from YouTubeMusic.Stream import get_stream
from YouTubeMusic.Video_Stream import get_video_audio_urls, stream_merged

# =========================
# 🔒 COOKIE PATH (ABSOLUTE)
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIE_PATH = os.path.join(BASE_DIR, "cookies.txt")


# =========================
# 🎵 AUDIO PROCESS (MP3)
# =========================

async def process_audio(video_url: str, video_id: str):

    filename = f"{video_id}.mp3"

    # 🔎 Duplicate Check
    existing = await get_existing_file(filename)
    if existing:
        return str(existing["_id"])

    # 🎧 Get direct audio stream URL
    stream_url = await get_stream(video_url, COOKIE_PATH)
    if not stream_url:
        raise Exception("Failed to extract audio stream")

    output_file = f"/tmp/{uuid.uuid4()}.mp3"

    # 🔥 Convert to MP3 using FFmpeg
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-loglevel", "error",
        "-i", stream_url,
        "-vn",
        "-ab", "192k",
        "-ar", "44100",
        "-y",
        output_file,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    await process.communicate()

    if process.returncode != 0:
        raise Exception("FFmpeg audio conversion failed")

    # 📦 Upload to Mongo GridFS
    upload_stream = await fs.open_upload_stream(
        filename,
        metadata={"contentType": "audio/mpeg"}
    )

    try:
        with open(output_file, "rb") as f:
            while chunk := f.read(1024 * 1024):
                await upload_stream.write(chunk)

        file_id = upload_stream._id

    finally:
        await upload_stream.close()
        if os.path.exists(output_file):
            os.remove(output_file)

    return str(file_id)


# =========================
# 🎬 VIDEO PROCESS (MP4 MERGED)
# =========================

async def process_video(video_url: str, video_id: str):

    filename = f"{video_id}.mp4"

    # 🔎 Duplicate Check
    existing = await get_existing_file(filename)
    if existing:
        return str(existing["_id"])

    # 🎬 Get separate streams
    video_stream, audio_stream = await get_video_audio_urls(
        video_url,
        COOKIE_PATH
    )

    if not video_stream or not audio_stream:
        raise Exception("Failed to extract video/audio streams")

    # 🔥 Merge streams using ffmpeg pipe
    process = await stream_merged(video_stream, audio_stream)
    if not process:
        raise Exception("FFmpeg merge process failed to start")

    # 📦 Upload directly from pipe to Mongo
    upload_stream = await fs.open_upload_stream(
        filename,
        metadata={"contentType": "video/mp4"}
    )

    try:
        while True:
            chunk = await process.stdout.read(1024 * 1024)
            if not chunk:
                break
            await upload_stream.write(chunk)

        await process.wait()

        if process.returncode != 0:
            raise Exception("FFmpeg video merge failed")

        file_id = upload_stream._id

    finally:
        await upload_stream.close()

    return str(file_id)

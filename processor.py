import asyncio
import os
import uuid
from mongo import fs
from YouTubeMusic.Stream import get_stream
from YouTubeMusic.Video_Stream import get_video_audio_urls, stream_merged


# =========================
# 🎵 AUDIO PROCESS (MP3)
# =========================

async def process_audio(video_url: str, video_id: str):

    # 🔎 Duplicate Check
    existing = fs.find_one({"filename": f"{video_id}.mp3"})
    if existing:
        return str(existing._id)

    # 🎧 Get direct audio stream URL
    stream_url = await get_stream(video_url)
    if not stream_url:
        return None

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
        return None

    # 📦 Store in Mongo
    with open(output_file, "rb") as f:
        file_id = fs.put(
            f,
            filename=f"{video_id}.mp3",
            content_type="audio/mpeg"
        )

    os.remove(output_file)
    return str(file_id)


# =========================
# 🎬 VIDEO PROCESS (MP4 MERGED)
# =========================

async def process_video(video_url: str, video_id: str):

    # 🔎 Duplicate Check
    existing = fs.find_one({"filename": f"{video_id}.mp4"})
    if existing:
        return str(existing._id)

    # 🎬 Get separate video + audio URLs
    video_stream, audio_stream = await get_video_audio_urls(video_url)
    if not video_stream or not audio_stream:
        return None

    # 🔥 Merge using stream_merged (pipe output)
    process = await stream_merged(video_stream, audio_stream)
    if not process:
        return None

    # 📦 Store merged MP4 directly from pipe into Mongo
    file = fs.new_file(
        filename=f"{video_id}.mp4",
        content_type="video/mp4"
    )

    try:
        while True:
            chunk = await process.stdout.read(1024 * 1024)  # 1MB chunk
            if not chunk:
                break
            file.write(chunk)

        await process.wait()

        if process.returncode != 0:
            file.close()
            fs.delete(file._id)
            return None

        file.close()
        return str(file._id)

    except Exception:
        file.close()
        fs.delete(file._id)
        return None

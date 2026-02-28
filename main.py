from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from bson import ObjectId

from YouTubeMusic.Search import Search, Trending, Suggest
from processor import process_audio, process_video
from mongo import fs

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- SEARCH ----------------

@app.get("/search")
async def search(q: str):
    data = await Search(q, limit=1)
    return JSONResponse(data)


# ---------------- TRENDING ----------------

@app.get("/trending")
async def trending():
    data = await Trending(limit=10)
    return {"results": data}


# ---------------- SUGGEST ----------------

@app.get("/suggest")
async def suggest(q: str):
    data = await Suggest(q, limit=5)
    return {"results": data}


# ---------------- GENERATE AUDIO ----------------

@app.get("/generate/audio/{video_id}")
async def generate_audio(video_id: str):
    url = f"https://www.youtube.com/watch?v={video_id}"
    mongo_id = await process_audio(url, video_id)

    if not mongo_id:
        raise HTTPException(500, "Audio processing failed")

    return {"file_id": mongo_id}


# ---------------- GENERATE VIDEO ----------------

@app.get("/generate/video/{video_id}")
async def generate_video(video_id: str):
    url = f"https://www.youtube.com/watch?v={video_id}"
    mongo_id = await process_video(url, video_id)

    if not mongo_id:
        raise HTTPException(500, "Video processing failed")

    return {"file_id": mongo_id}


# ---------------- STREAM AUDIO ----------------

@app.get("/audio/{file_id}")
async def stream_audio(file_id: str):

    file = fs.get(ObjectId(file_id))
    if not file:
        raise HTTPException(404, "Audio not found")

    return StreamingResponse(
        file,
        media_type="audio/mpeg",
        headers={"Accept-Ranges": "bytes"},
    )


# ---------------- STREAM VIDEO ----------------

@app.get("/video/{file_id}")
async def stream_video(file_id: str):

    file = fs.get(ObjectId(file_id))
    if not file:
        raise HTTPException(404, "Video not found")

    return StreamingResponse(
        file,
        media_type="video/mp4",
        headers={"Accept-Ranges": "bytes"},
    )

@app.on_event("startup")
async def startup():
    from mongo import init_indexes
    await init_indexes()

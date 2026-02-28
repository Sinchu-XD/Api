import os
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

# =========================
# 🔗 CONFIG
# =========================

MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb+srv://Vexera:Vexera@vexera.wtrsmyc.mongodb.net/?retryWrites=true&w=majority"
)

DB_NAME = os.getenv("MONGO_DB", "media_db")

# =========================
# 🚀 CLIENT INIT
# =========================

client = AsyncIOMotorClient(
    MONGO_URL,
    maxPoolSize=50,
    minPoolSize=5,
    serverSelectionTimeoutMS=5000,
)

db = client[DB_NAME]

# ✅ Correct Async GridFS
fs = AsyncIOMotorGridFSBucket(db)

# =========================
# 🔎 INDEX INIT
# =========================

async def init_indexes():
    await db.fs.files.create_index("filename")
    await db.fs.files.create_index("uploadDate")


# =========================
# 🔍 HELPERS (Async)
# =========================

async def get_existing_file(filename: str):
    return await db.fs.files.find_one({"filename": filename})

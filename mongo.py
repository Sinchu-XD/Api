import os
from motor.motor_asyncio import AsyncIOMotorClient
import gridfs

# =========================
# 🔗 CONFIG
# =========================

MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://Vexera:Vexera@vexera.wtrsmyc.mongodb.net/?appName=Vexera")
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

# GridFS for large files (audio/video)
fs = gridfs.GridFS(db)

# =========================
# 🔎 INDEX (Prevent Duplicate Processing)
# =========================

async def init_indexes():
    await db.fs.files.create_index("filename", unique=False)
    await db.fs.files.create_index("uploadDate")


# =========================
# 🔍 HELPERS
# =========================

def get_existing_file(filename: str):
    """
    Check if file already exists in GridFS
    """
    return fs.find_one({"filename": filename})

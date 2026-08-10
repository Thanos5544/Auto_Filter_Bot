import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from typing import Dict, List
from collections import defaultdict
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from utils import get_settings, save_group_settings
from info import (
    COLLECTION_NAME, COVERX, DATABASE_NAME, DATABASE_URI, DATABASE_URI2, DATABASE_URI3,
    INDEX_CAPTION, MAX_B_TN, MULTIPLE_DB, ULTRA_FAST_MODE, USE_CAPTION_FILTER,
)
from datetime import datetime, timedelta
import asyncio
from functools import lru_cache

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---- LIMITS ----
PRIMARY_LIMIT = 407
SECONDARY_LIMIT = 480

_db_stats_cache = {}

@lru_cache(maxsize=4096)
def compile_regex(pattern):
    return re.compile(pattern, re.IGNORECASE)

client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

if MULTIPLE_DB and DATABASE_URI2:
    client2 = AsyncIOMotorClient(DATABASE_URI2)
    db2 = client2[DATABASE_NAME]
    instance2 = Instance.from_db(db2)
else:
    client2 = client
    db2 = db
    instance2 = instance

if MULTIPLE_DB and DATABASE_URI3:
    client3 = AsyncIOMotorClient(DATABASE_URI3)
    db3 = client3[DATABASE_NAME]
    instance3 = Instance.from_db(db3)
else:
    client3 = client
    db3 = db
    instance3 = instance

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    cover = fields.StrField(allow_none=True)
    class Meta:
        indexes = ("$file_name",)
        collection_name = COLLECTION_NAME

@instance2.register
class Media2(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    cover = fields.StrField(allow_none=True)
    class Meta:
        indexes = ("$file_name",)
        collection_name = COLLECTION_NAME

@instance3.register
class Media3(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    cover = fields.StrField(allow_none=True)
    class Meta:
        indexes = ("$file_name",)
        collection_name = COLLECTION_NAME

async def check_db_size(db_obj):
    try:
        key = id(db_obj)
        now = datetime.utcnow()
        if key in _db_stats_cache:
            ts, size = _db_stats_cache[key]
            if now - ts < timedelta(seconds=30):
                return size
        stats = await db_obj.command("dbstats")
        size_mb = (stats["dataSize"] + stats["indexSize"]) / (1024 * 1024)
        _db_stats_cache[key] = (now, size_mb)
        return size_mb
    except Exception:
        logger.exception("Error checking database size")
        return 0

async def save_file(media):
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"[_\-\.#+$%^&*()!~`,;:\"'?/<>\[\]{}=|\\]", " ", str(media.file_name))
    file_name = re.sub(r"\s+", " ", file_name).strip()
    saveMedia = Media
    target_db = "Primary"
    if MULTIPLE_DB:
        try:
            if await Media.find_one({"file_id": file_id}):
                return False, 0
            if DATABASE_URI2 and await Media2.find_one({"file_id": file_id}):
                return False, 0
            if DATABASE_URI3 and await Media3.find_one({"file_id": file_id}):
                return False, 0
            primary_db_size = await check_db_size(db)
            if primary_db_size >= PRIMARY_LIMIT:
                if DATABASE_URI3:
                    secondary_db_size = await check_db_size(db2)
                    if secondary_db_size >= SECONDARY_LIMIT:
                        saveMedia = Media3
                        target_db = "Tertiary"
                    else:
                        saveMedia = Media2
                        target_db = "Secondary"
                else:
                    saveMedia = Media2
                    target_db = "Secondary"
        except Exception as e:
            logger.error("Error during MULTIPLE_DB check", exc_info=e)
    try:
        cover_to_use = getattr(getattr(media, "cover", None), "file_id", None)
        record = saveMedia(
            file_id=file_id, file_ref=file_ref, file_name=file_name,
            file_size=media.file_size, file_type=media.file_type,
            mime_type=media.mime_type,
            caption=(media.caption.html if media.caption and INDEX_CAPTION else None),
            cover=cover_to_use if COVERX else None,
        )
    except Exception as e:
        logger.exception(f"[ERROR] '{file_name}' → {e}")
        return False, 2
    try:
        await record.commit()
    except DuplicateKeyError:
        return False, 0
    except Exception as e:
        logger.exception(f"[ERROR] Failed commit of '{file_name}' to {target_db} DB.", exc_info=e)
        return False, 3
    return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=None, offset=0, filter=False):
    if chat_id is not None and max_results is None:
        settings = await get_settings(int(chat_id))
        if "max_btn" not in settings:
            await save_group_settings(int(chat_id), "max_btn", True)
            settings["max_btn"] = True
        max_results = 10 if settings["max_btn"] else int(MAX_B_TN)

    def _extract_base(filename: str) -> str:
        try:
            year_match = re.search(r"^(.*?(\d{4}|\(\d{4}\)))", filename, re.IGNORECASE)
            if year_match:
                t = year_match.group(1).replace("(", "").replace(")", "")
                t = re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", t).strip().lower()
                return t
            season_match = re.search(r"(.*?)(?:S(\d{1,2})|Season\s*(\d+)|Season(\d+))(?:\s*Combined)?", filename, re.IGNORECASE)
            if season_match:
                t = season_match.group(1).strip()
                t = re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", t).strip().lower()
                return t
            t = re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", filename).strip().lower()
            t = re.sub(r"\.(mp4|mkv|avi|mov|flv|webm)$", "", t)
            t = re.split(r"\s+(1080p|720p|480p|2160p|4k|hevc|x264|x265|web-dl|bluray|hdr|esub)\b", t)[0]
            return t.strip()
        except:
            return filename.lower()

    def _get_season_ep(fn: str):
        m = re.search(r"S0*(\d{1,2})(?:\s*E0*(\d{1,2}))?", fn, re.I)
        if m:
            s = int(m.group(1)) if m.group(1) else 999
            e = int(m.group(2)) if m.group(2) else 999
            return (s, e)
        m2 = re.search(r"Season\s*0*(\d{1,2})", fn, re.I)
        if m2:
            return (int(m2.group(1)), 999)
        return (999, 999)

    def _score(fn, q):
        fn_low = fn.lower()
        q_low = q.lower().strip()
        if not q_low:
            return 0
        # Season parse for query like "from s03" or "dark s02"
        q_season = None
        q_base = q_low
        mqs = re.search(r"\bS0*(\d{1,2})\b", q_low)
        if mqs:
            q_season = int(mqs.group(1))
            q_base = re.sub(r"\bS0*\d{1,2}\b", "", q_low, flags=re.I)
            q_base = re.sub(r"\bseason\s*0*\d{1,2}\b", "", q_base, flags=re.I)
            q_base = re.sub(r"\s+", " ", q_base).strip()
            if not q_base:
                q_base = q_low

        base = _extract_base(fn)
        f_season, f_ep = _get_season_ep(fn)

        # If query has season filter, mismatch goes to bottom
        if q_season is not None and f_season != q_season:
            return 10  # bahut neeche

        norm_fn = re.sub(r"[._\-\[\]()]+", " ", fn_low)
        norm_fn = re.sub(r"\s+", " ", norm_fn).strip()
        if q_base and norm_fn == q_base:
            return 1000
        if q_base and base == q_base:
            return 990
        if q_base and base.startswith(q_base + " "):
            q_words = q_base.split()
            base_words = base.split()
            extra = len(base_words) - len(q_words)
            if extra < 0:
                extra = 0
            meta_pat = re.compile(r"^(s\d{1,2}|season\d+|e\d{1,2}|\d{4}|1080p|720p|480p|2160p|4k|hevc|x264|x265|web|bluray|hdrip|esub)$")
            penalty = 0
            if extra > 0:
                extra_words = base_words[len(q_words):]
                for w in extra_words:
                    if meta_pat.match(w):
                        penalty += 5
                    else:
                        penalty += 50
            return 900 - penalty - len(fn)*0.005
        if q_base and fn_low.startswith(q_base):
            return 850
        if q_base and re.search(rf"(\b|[\.\s\-\+_]){re.escape(q_base)}(\b|[\.\s\-\+_])", fn_low):
            pos = fn_low.find(q_base)
            return 700 - pos*0.1
        if q_base and q_base in fn_low:
            return 600 - fn_low.find(q_base)*0.1
        if q_base:
            q_words = q_base.split()
            if all(w in fn_low for w in q_words):
                return 500
            if any(w in fn_low for w in q_words):
                return 100
        return 0

    # Handle list query with AND logic
    if isinstance(query, list):
        terms = [q.strip() for q in query if q and q.strip()]
        if not terms:
            return [], None, 0
        clean_query = " ".join(terms)
        and_filters = []
        for term in terms:
            if " " in term:
                words = [re.escape(w) for w in term.split() if w]
                pat = r".*[\s\.\+\-_]" .join(words)
            else:
                pat = r"(\b|[\.\+\-_])" + re.escape(term) + r"(\b|[\.\+\-_])"
            try:
                rgx = compile_regex(pat)
            except re.error:
                continue
            if USE_CAPTION_FILTER:
                and_filters.append({"$or": [{"file_name": rgx}, {"caption": rgx}]})
            else:
                and_filters.append({"file_name": rgx})
        if not and_filters:
            return [], None, 0
        filter_mongo = {"$and": and_filters} if len(and_filters) > 1 else and_filters[0]
    else:
        query = query.strip()
        if not query:
            return [], None, 0
        clean_query = query
        if " " in query:
            words = [re.escape(w) for w in query.split() if w]
            raw_pattern = r".*[\s\.\+\-_]" .join(words)
        else:
            raw_pattern = r"(\b|[\.\+\-_])" + re.escape(query) + r"(\b|[\.\+\-_])"
        try:
            regex = compile_regex(raw_pattern)
        except re.error:
            return [], None, 0
        filter_mongo = {"$or": [{"file_name": regex},{"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}

    if file_type:
        filter_mongo["file_type"] = file_type

    # FAST but Accurate - 55 per DB, total ~165, then exact sort
    fetch_extra = 5
    limit = (max_results + 1) * fetch_extra
    if ULTRA_FAST_MODE:
        fetch_limit = offset + limit
        if MULTIPLE_DB:
            if DATABASE_URI3:
                r = await asyncio.gather(
                    Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                    Media2.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                    Media3.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                )
                files = r[2] + r[1] + r[0]
            else:
                r = await asyncio.gather(
                    Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                    Media2.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                )
                files = r[1] + r[0]
        else:
            files = await Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit)
        files.sort(key=lambda f: (-_score(getattr(f, 'file_name',''), clean_query), _get_season_ep(getattr(f, 'file_name',''))))
        files = files[offset:offset+max_results+1]
        has_next = len(files) > max_results
        if has_next:
            files = files[:-1]
        next_offset = offset + len(files) if has_next else ""
        total_results = offset + len(files) + (1 if has_next else 0)
    else:
        fetch_limit = offset + limit
        if MULTIPLE_DB:
            if DATABASE_URI3:
                count_results, find_results = await asyncio.gather(
                    asyncio.gather(Media.count_documents(filter_mongo), Media2.count_documents(filter_mongo), Media3.count_documents(filter_mongo)),
                    asyncio.gather(
                        Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                        Media2.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                        Media3.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                    )
                )
                total_results = sum(count_results)
                files = find_results[2] + find_results[1] + find_results[0]
            else:
                count_results, find_results = await asyncio.gather(
                    asyncio.gather(Media.count_documents(filter_mongo), Media2.count_documents(filter_mongo)),
                    asyncio.gather(
                        Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                        Media2.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit),
                    )
                )
                total_results = sum(count_results)
                files = find_results[1] + find_results[0]
            files.sort(key=lambda f: (-_score(getattr(f, 'file_name',''), clean_query), _get_season_ep(getattr(f, 'file_name',''))))
            files = files[offset:offset+max_results]
        else:
            total_results = await Media.count_documents(filter_mongo)
            files = await Media.find(filter_mongo).sort("$natural", -1).limit(fetch_limit).to_list(length=fetch_limit)
            files.sort(key=lambda f: (-_score(getattr(f, 'file_name',''), clean_query), _get_season_ep(getattr(f, 'file_name',''))))
            files = files[offset:offset+max_results]
        next_offset = offset + len(files)
        if next_offset >= total_results:
            next_offset = ""
    return files, next_offset, total_results

async def get_bad_files(query, file_type=None):
    query = query.strip()
    if not query: return [], 0
    if " " not in query: raw_pattern = r"(\b|[\.\+\-_])" + re.escape(query) + r"(\b|[\.\+\-_])"
    else: raw_pattern = r".*[\s\.\+\-_]".join(map(re.escape, query.split()))
    try: regex = compile_regex(raw_pattern)
    except re.error: return [], 0
    filter_mongo = {"$or": [{"file_name": regex},{"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}
    if file_type: filter_mongo["file_type"] = file_type
    tasks = [Media.find(filter_mongo).sort("$natural", -1).to_list(300)]
    if MULTIPLE_DB:
        tasks.append(Media2.find(filter_mongo).sort("$natural", -1).to_list(300))
        if DATABASE_URI3: tasks.append(Media3.find(filter_mongo).sort("$natural", -1).to_list(300))
    results = await asyncio.gather(*tasks)
    if MULTIPLE_DB and len(results)==3: files = results[2]+results[1]+results[0]
    elif MULTIPLE_DB and len(results)>1: files = results[1]+results[0]
    else: files = results[0]
    return files[:300], len(files[:300])

async def get_file_details(query):
    filter = {"file_id": query}
    tasks = [Media.find(filter).to_list(length=1)]
    if MULTIPLE_DB:
        tasks.append(Media2.find(filter).to_list(length=1))
        if DATABASE_URI3: tasks.append(Media3.find(filter).to_list(length=1))
    results = await asyncio.gather(*tasks)
    for f in results:
        if f: return f
    return []

def encode_file_id(s: bytes) -> str:
    r = b""; n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")
def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")
def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash))
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
async def dreamxbotz_fetch_media(limit: int) -> List[dict]:
    try:
        if MULTIPLE_DB:
            if await check_db_size(db) > PRIMARY_LIMIT:
                if DATABASE_URI3 and await check_db_size(db2) > SECONDARY_LIMIT:
                    return await Media3.find().sort("$natural", -1).limit(limit).to_list(length=limit)
                return await Media2.find().sort("$natural", -1).limit(limit).to_list(length=limit)
        return await Media.find().sort("$natural", -1).limit(limit).to_list(length=limit)
    except Exception as e:
        logger.error(f"Error in dreamxbotz_fetch_media: {e}")
        return []

async def dreamxbotz_clean_title(filename: str, is_series: bool = False) -> str:
    try:
        year_match = re.search(r"^(.*?(\d{4}|\(\d{4}\)))", filename, re.IGNORECASE)
        if year_match:
            title = year_match.group(1).replace("(", "").replace(")", "")
            return re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", title).strip().title()
        if is_series:
            season_match = re.search(r"(.*?)(?:S(\d{1,2})|Season\s*(\d+)|Season(\d+))(?:\s*Combined)?", filename, re.IGNORECASE)
            if season_match:
                title = season_match.group(1).strip()
                season = season_match.group(2) or season_match.group(3) or season_match.group(4)
                title = re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", title).strip().title()
                return f"{title} S{int(season):02}"
        return re.sub(r"(?:@[^ \n\r\t.,:;!?()\[\]{}<>\\\/\"'=_%]+|[._\-\[\]@()]+)", " ", filename).strip().title()
    except Exception as e:
        logger.error(f"Error in truncate_title: {e}")
        return filename

async def dreamxbotz_get_movies(limit: int = 20) -> List[str]:
    try:
        cursor = await dreamxbotz_fetch_media(limit * 2)
        results = set()
        pattern = r"(?:s\d{1,2}|season\s*\d+|season\d+)(?:\s*combined)?(?:e\d{1,2}|episode\s*\d+)?\b"
        for file in cursor:
            file_name = getattr(file, "file_name", "")
            if not re.search(pattern, file_name, re.IGNORECASE):
                title = await dreamxbotz_clean_title(file_name)
                results.add(title)
            if len(results) >= limit: break
        return sorted(list(results))[:limit]
    except Exception as e:
        logger.error(f"Error in dreamxbotz_get_movies: {e}")
        return []

async def dreamxbotz_get_series(limit: int = 30) -> Dict[str, List[int]]:
    try:
        cursor = await dreamxbotz_fetch_media(limit * 5)
        grouped = defaultdict(list)
        pattern = r"(.*?)(?:S(\d{1,2})|Season\s*(\d+)|Season(\d+))(?:\s*Combined)?(?:E(\d{1,2})|Episode\s*(\d+))?\b"
        for file in cursor:
            file_name = getattr(file, "file_name", "")
            match = re.search(pattern, file_name, re.IGNORECASE)
            if match:
                title = await dreamxbotz_clean_title(match.group(1), is_series=True)
                season = int(match.group(2) or match.group(3) or match.group(4))
                grouped[title].append(season)
        return {title: sorted(set(seasons))[:10] for title, seasons in grouped.items() if seasons}
    except Exception as e:
        logger.error(f"Error in dreamxbotz_get_series: {e}")
        return []

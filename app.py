import asyncio
import base64
import json
import os
import re
import time
from urllib.parse import urlparse

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse


SITE_ORIGIN = os.getenv("SITE_ORIGIN", "https://tesla-screen-check-26.ar9999.chatgpt.site")
VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
MAX_QUERY = 100
cache = {}
cache_lock = asyncio.Lock()
app = FastAPI(title="Video source service", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[SITE_ORIGIN, "http://127.0.0.1:8766", "http://localhost:8766"],
    allow_methods=["GET", "HEAD"],
    allow_headers=["Range", "Content-Type"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)


def clean_id(video_id):
    if not VIDEO_ID.fullmatch(video_id or ""):
        raise HTTPException(400, "Invalid video ID")
    return video_id


def extract(url, flat=False):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": flat,
        "js_runtimes": {"node": {}},
        "socket_timeout": 12,
        "retries": 1,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


def item(entry):
    video_id = entry.get("id") or entry.get("url", "").split("v=")[-1]
    if not VIDEO_ID.fullmatch(video_id or ""):
        return None
    return {
        "id": video_id,
        "title": entry.get("title") or "Untitled video",
        "channel": entry.get("channel") or entry.get("uploader") or "YouTube",
        "duration": entry.get("duration"),
        "views": entry.get("view_count"),
        "thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
    }


def choose_formats(info):
    formats = info.get("formats") or []
    videos = [f for f in formats if f.get("protocol") == "https" and f.get("ext") == "mp4"
              and str(f.get("vcodec", "none")).startswith("avc1") and f.get("acodec") == "none"
              and f.get("height") and f.get("height") <= 720 and f.get("url")]
    audios = [f for f in formats if f.get("protocol") == "https" and f.get("acodec") != "none"
              and f.get("vcodec") == "none" and f.get("ext") in ("m4a", "mp4") and f.get("url")]
    videos.sort(key=lambda f: (f.get("height") or 0, f.get("tbr") or 0), reverse=True)
    audios.sort(key=lambda f: f.get("abr") or 0, reverse=True)
    if not videos or not audios:
        raise HTTPException(422, "This video has no supported clear media streams")
    return videos[0], audios[0]


async def info_for(video_id):
    clean_id(video_id)
    now = time.monotonic()
    async with cache_lock:
        hit = cache.get(video_id)
        if hit and hit[0] > now:
            return hit[1]
    try:
        info = await asyncio.wait_for(asyncio.to_thread(extract, f"https://www.youtube.com/watch?v={video_id}"), 35)
    except Exception as exc:
        raise HTTPException(502, f"Could not resolve this video: {type(exc).__name__}") from exc
    async with cache_lock:
        cache[video_id] = (now + 1200, info)
        if len(cache) > 80:
            for key in list(cache)[:20]:
                cache.pop(key, None)
    return info


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/search")
async def search(q: str):
    q = q.strip()[:MAX_QUERY]
    if not q:
        raise HTTPException(400, "Search query required")
    try:
        result = await asyncio.wait_for(asyncio.to_thread(extract, f"ytsearch18:{q}", True), 30)
    except Exception as exc:
        raise HTTPException(502, f"Search unavailable: {type(exc).__name__}") from exc
    return {"items": [v for e in result.get("entries", []) if (v := item(e))]}


@app.get("/api/video/{video_id}")
async def video(video_id: str):
    info = await info_for(video_id)
    result = item(info)
    video_format, audio_format = choose_formats(info)
    result.update({
        "description": (info.get("description") or "")[:4000],
        "channelUrl": info.get("channel_url"),
        "videoHeight": video_format.get("height"),
        "audioBitrate": audio_format.get("abr"),
    })
    return result


@app.get("/api/suggested/{video_id}")
async def suggested(video_id: str):
    info = await info_for(video_id)
    query = " ".join((info.get("title") or "").split()[:6])
    result = await search(query)
    return {"items": [v for v in result["items"] if v["id"] != video_id]}


@app.get("/api/media/{video_id}/{kind}")
async def media(video_id: str, kind: str, request: Request):
    if kind not in ("video", "audio"):
        raise HTTPException(400, "Invalid media kind")
    info = await info_for(video_id)
    vf, af = choose_formats(info)
    media_url = (vf if kind == "video" else af)["url"]
    host = urlparse(media_url).hostname or ""
    if host != "googlevideo.com" and not host.endswith(".googlevideo.com"):
        raise HTTPException(502, "Unexpected media host")
    headers = {}
    if request.headers.get("range"):
        headers["Range"] = request.headers["range"]
    client = httpx.AsyncClient(timeout=httpx.Timeout(30, connect=8), follow_redirects=False)
    try:
        upstream = await client.send(client.build_request("GET", media_url, headers=headers), stream=True)
    except Exception as exc:
        await client.aclose()
        raise HTTPException(502, "Media source unavailable") from exc
    if upstream.status_code not in (200, 206):
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(502, f"Media source returned {upstream.status_code}")

    async def chunks():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    response_headers = {k: v for k, v in upstream.headers.items()
                        if k.lower() in ("content-range", "content-length", "accept-ranges", "content-type")}
    response_headers["cache-control"] = "private, max-age=60"
    return StreamingResponse(chunks(), status_code=upstream.status_code,
                             media_type=upstream.headers.get("content-type", "application/octet-stream"),
                             headers=response_headers)


@app.get("/api/frames/{video_id}")
async def frames(video_id: str):
    info = await info_for(video_id)
    vf, _ = choose_formats(info)
    media_url = vf["url"]
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-i", media_url,
        "-vf", "fps=10,scale=640:-2", "-q:v", "8", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )

    async def frame_lines():
        buffer = bytearray()
        index = 0
        try:
            while True:
                chunk = await process.stdout.read(64 * 1024)
                if not chunk:
                    break
                buffer.extend(chunk)
                while True:
                    start = buffer.find(b"\xff\xd8")
                    end = buffer.find(b"\xff\xd9", start + 2) if start >= 0 else -1
                    if start < 0 or end < 0:
                        break
                    jpeg = bytes(buffer[start:end + 2])
                    del buffer[:end + 2]
                    if len(jpeg) < 400_000:
                        yield json.dumps({"t": index / 10, "jpeg": base64.b64encode(jpeg).decode("ascii")}) + "\n"
                        index += 1
                if len(buffer) > 1_000_000:
                    buffer.clear()
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()

    return StreamingResponse(frame_lines(), media_type="application/x-ndjson",
                             headers={"cache-control": "no-store", "x-accel-buffering": "no"})

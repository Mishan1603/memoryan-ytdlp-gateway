import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import time
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

log = logging.getLogger("memoryan-ytdlp-gateway")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

ALLOWED_PREVIEW_HOSTS = frozenset(
    h.strip().lower()
    for h in os.getenv(
        "ALLOWED_PREVIEW_HOSTS",
        "tiktok.com,www.tiktok.com,vm.tiktok.com,vt.tiktok.com,"
        "instagram.com,www.instagram.com",
    ).split(",")
    if h.strip()
)

EDGE_SECRET = os.getenv("EDGE_SHARED_SECRET", "").strip()
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "20"))
MAX_THUMB_BYTES = int(os.getenv("MAX_THUMB_BYTES", str(5 * 1024 * 1024)))

app = FastAPI(title="Memoryan yt-dlp gateway", version="1.0.0")


class PreviewBody(BaseModel):
    url: str = Field(..., min_length=4, max_length=2048)


class ThumbnailFromUrlBody(BaseModel):
    url: str = Field(..., min_length=4, max_length=2048)


def _host_allowed(url: str, allowed: frozenset[str]) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    h = host.lower()
    if h in allowed:
        return True
    for a in allowed:
        if h == a or h.endswith("." + a):
            return True
    return False


def _require_edge_secret(x_memoryan_edge_secret: str | None) -> None:
    if not EDGE_SECRET:
        raise HTTPException(status_code=500, detail="EDGE_SHARED_SECRET not configured")
    if not x_memoryan_edge_secret or x_memoryan_edge_secret != EDGE_SECRET:
        raise HTTPException(status_code=401, detail="invalid edge secret")


def _extract_json_obj(stdout: str) -> dict:
    stdout = stdout.strip()
    if not stdout:
        return {}
    # yt-dlp may print warnings before JSON; take last {...} block
    brace = stdout.rfind("{")
    if brace == -1:
        return {}
    chunk = stdout[brace:]
    try:
        return json.loads(chunk)
    except json.JSONDecodeError:
        return {}


def _run_ytdlp_json(url: str) -> dict:
    cmd = [
        "yt-dlp",
        "--no-download",
        "--no-warnings",
        "--skip-download",
        "--dump-single-json",
        "--no-playlist",
        url,
    ]
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=int(os.getenv("YTDLP_TIMEOUT_S", "25")),
    )
    dt_ms = int((time.perf_counter() - t0) * 1000)
    log.info("yt-dlp finished in %sms exit=%s", dt_ms, proc.returncode)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()[:500]
        log.warning("yt-dlp error: %s", err)
        return {}
    return _extract_json_obj(proc.stdout or "")


def _pick_thumb_url(info: dict) -> str:
    for k in ("thumbnail", "og:image"):
        v = info.get(k)
        if isinstance(v, str) and v.startswith("http"):
            return v
    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list) and thumbs:
        last = thumbs[-1]
        if isinstance(last, dict):
            u = last.get("url")
            if isinstance(u, str) and u.startswith("http"):
                return u
    return ""


def _sanitize_text(s: str, max_len: int) -> str:
    if not s:
        return ""
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()[:max_len]


async def _run_ytdlp_json_async(url: str) -> dict:
    return await asyncio.to_thread(_run_ytdlp_json, url)


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/v1/preview")
async def v1_preview(
    body: PreviewBody,
    x_memoryan_edge_secret: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    _require_edge_secret(x_memoryan_edge_secret)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer")

    url = body.url.strip()
    if not url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="invalid url")

    if not _host_allowed(url, ALLOWED_PREVIEW_HOSTS):
        raise HTTPException(status_code=400, detail="host not allowed for preview")

    info = await _run_ytdlp_json_async(url)
    title = _sanitize_text(str(info.get("title") or ""), 500)
    desc = _sanitize_text(str(info.get("description") or ""), 8000)
    thumb = _pick_thumb_url(info)
    site = _sanitize_text(str(info.get("extractor") or info.get("ie_key") or ""), 120)

    log.info("[ytdlp-gateway] preview ok url=%s title_len=%s desc_len=%s thumb=%s", url, len(title), len(desc), bool(thumb))

    return JSONResponse(
        {
            "url": url,
            "title": title or "Unknown title",
            "description": desc or "No description available",
            "imageUrl": thumb,
            "siteName": site or "Video",
            "source": "ytdlp-gateway",
        }
    )


@app.post("/v1/thumbnail-from-url")
async def v1_thumbnail_from_url(
    body: ThumbnailFromUrlBody,
    x_memoryan_edge_secret: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    _require_edge_secret(x_memoryan_edge_secret)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer")

    url = body.url.strip()
    if not url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="invalid url")

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT_S,
        follow_redirects=True,
        headers={"User-Agent": "MemoryanThumbnailFetcher/1.0"},
    ) as client:
        r = await client.get(url)
        r.raise_for_status()
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        if ct not in ("image/jpeg", "image/png", "image/webp", "image/gif"):
            raise HTTPException(status_code=415, detail=f"unsupported content-type: {ct or 'unknown'}")
        data = r.content
        if len(data) > MAX_THUMB_BYTES:
            raise HTTPException(status_code=413, detail="image too large")

    log.info("[ytdlp-gateway] thumbnail-from-url bytes=%s ct=%s", len(data), ct)

    b64 = base64.b64encode(data).decode("ascii")
    return JSONResponse(
        {
            "url": url,
            "title": "",
            "description": "",
            "imageUrl": f"data:{ct};base64,{b64}",
            "siteName": "Image",
            "source": "thumbnail-http",
        }
    )

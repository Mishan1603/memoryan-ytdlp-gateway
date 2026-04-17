import asyncio
import base64
import contextlib
import json
import logging
import os
import re
import shlex
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
# Keep total request time well under Railway edge limits; cold start + yt-dlp must fit.
YTDLP_SUBPROCESS_TIMEOUT_S = int(os.getenv("YTDLP_TIMEOUT_S", "18"))
YTDLP_SOCKET_TIMEOUT_S = int(os.getenv("YTDLP_SOCKET_TIMEOUT_S", "12"))


@contextlib.asynccontextmanager
async def _lifespan(_app: FastAPI):
    port = os.getenv("PORT", "")
    log.info(
        "startup PORT=%s EDGE_SHARED_SECRET=%s",
        port or "(unset — local default may differ from Railway)",
        "set" if EDGE_SECRET else "MISSING",
    )
    try:
        v = subprocess.run(
            ["yt-dlp", "--version"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        out = (v.stdout or v.stderr or "").strip()
        log.info("yt-dlp binary ok: %s", out[:120] or f"exit={v.returncode}")
    except Exception as e:
        log.error("yt-dlp binary check failed: %s", e)
    yield


app = FastAPI(title="Memoryan yt-dlp gateway", version="1.0.0", lifespan=_lifespan)


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


def _first_json_object(s: str) -> dict:
    """Parse the first top-level JSON object; handles extra text after yt-dlp's dump."""
    start = s.find("{")
    if start == -1:
        return {}
    depth = 0
    in_str = False
    escape = False
    quote = ""
    for i in range(start, len(s)):
        c = s[i]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == quote:
                in_str = False
            continue
        if c in "\"'":
            in_str = True
            quote = c
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                chunk = s[start : i + 1]
                try:
                    out = json.loads(chunk)
                    return out if isinstance(out, dict) else {}
                except json.JSONDecodeError:
                    return {}
    return {}


def _extract_json_obj(stdout: str) -> dict:
    if not (stdout or "").strip():
        return {}
    s = stdout.strip()
    # Single-line JSON (common for --dump-single-json)
    if s.startswith("{") and s.endswith("}"):
        try:
            out = json.loads(s)
            return out if isinstance(out, dict) else {}
        except json.JSONDecodeError:
            pass
    return _first_json_object(s)


def _preview_richness(ent: dict) -> int:
    if not isinstance(ent, dict):
        return 0
    n = 0
    if _pick_title(ent):
        n += 5
    if _pick_description(ent):
        n += 3
    if _pick_thumb_url(ent):
        n += 4
    if ent.get("formats"):
        n += 1
    return n


def _flatten_playlist_for_preview(info: dict) -> dict:
    """Use the richest video-like entry when yt-dlp returns a playlist (carousel, etc.)."""
    if info.get("_type") != "playlist":
        return info
    entries = info.get("entries")
    if not isinstance(entries, list) or not entries:
        return info
    dict_entries = [e for e in entries if isinstance(e, dict)]
    if not dict_entries:
        return info
    best = max(dict_entries, key=_preview_richness)
    merged = {**best}
    if isinstance(info.get("title"), str) and info["title"].strip() and not _pick_title(merged):
        merged["title"] = info["title"].strip()
    if isinstance(info.get("description"), str) and info["description"].strip() and not _pick_description(merged):
        merged["description"] = info["description"].strip()
    if not _pick_thumb_url(merged) and _pick_thumb_url(info):
        if isinstance(info.get("thumbnail"), str):
            merged["thumbnail"] = info["thumbnail"]
        elif isinstance(info.get("thumbnails"), list):
            merged["thumbnails"] = info["thumbnails"]
    return merged


def _run_ytdlp_json(url: str) -> dict:
    extra = os.getenv("YTDLP_EXTRA_ARGS", "").strip()
    cmd = [
        "yt-dlp",
        "--no-download",
        "--no-warnings",
        "--skip-download",
        "--dump-single-json",
        "--no-playlist",
        "--no-check-formats",
        "--retries",
        "1",
        "--fragment-retries",
        "1",
        "--socket-timeout",
        str(YTDLP_SOCKET_TIMEOUT_S),
    ]
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if "instagram." in host:
        cmd.extend(["--add-header", "Referer:https://www.instagram.com/"])
    elif "tiktok." in host:
        cmd.extend(["--add-header", "Referer:https://www.tiktok.com/"])
    if extra:
        try:
            cmd.extend(shlex.split(extra, posix=True))
        except ValueError as e:
            log.warning("YTDLP_EXTRA_ARGS shlex parse failed: %s", e)
    cmd.append(url)
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=YTDLP_SUBPROCESS_TIMEOUT_S,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        log.warning(
            "yt-dlp subprocess timeout after %ss url=%s",
            YTDLP_SUBPROCESS_TIMEOUT_S,
            url[:80],
        )
        return {}
    dt_ms = int((time.perf_counter() - t0) * 1000)
    log.info("yt-dlp finished in %sms exit=%s", dt_ms, proc.returncode)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()[:500]
        log.warning("yt-dlp error: %s", err)
        return {}
    raw = _extract_json_obj(proc.stdout or "")
    if not raw and (proc.stdout or "").strip():
        log.warning(
            "yt-dlp JSON parse got empty dict; stdout_len=%s stderr_head=%s",
            len(proc.stdout or ""),
            (proc.stderr or "")[:300],
        )
    return _flatten_playlist_for_preview(raw)


def _thumb_url_from_obj(obj: object) -> str:
    if isinstance(obj, str) and obj.startswith("http"):
        return obj
    if isinstance(obj, dict):
        u = obj.get("url") or obj.get("src")
        if isinstance(u, str) and u.startswith("http"):
            return u
    return ""


def _pick_thumb_url(info: dict) -> str:
    u = _thumb_url_from_obj(info.get("thumbnail"))
    if u:
        return u
    og = info.get("og:image")
    if isinstance(og, str) and og.startswith("http"):
        return og
    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list):
        for thumb in thumbs:
            u = _thumb_url_from_obj(thumb)
            if u:
                return u
    return ""


def _pick_title(info: dict) -> str:
    for k in ("title", "fulltitle", "track", "alt_title"):
        v = info.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    ch = info.get("channel") or info.get("uploader")
    vid = info.get("id") or info.get("display_id")
    if isinstance(ch, str) and ch.strip():
        return f"Video by {ch.strip()}"
    if isinstance(vid, str) and vid.strip():
        return f"Instagram {vid.strip()}"
    return ""


def _pick_description(info: dict) -> str:
    for k in ("description", "summary", "content"):
        v = info.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
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
    t_req = time.perf_counter()
    log.info("[ytdlp-gateway] /v1/preview start url=%s", (body.url or "")[:120])
    _require_edge_secret(x_memoryan_edge_secret)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer")

    url = body.url.strip()
    if not url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="invalid url")

    if not _host_allowed(url, ALLOWED_PREVIEW_HOSTS):
        raise HTTPException(status_code=400, detail="host not allowed for preview")

    info = await _run_ytdlp_json_async(url)
    title = _sanitize_text(_pick_title(info), 500)
    if not title and "instagram." in (urlparse(url).hostname or "").lower():
        m = re.search(r"/(?:p|reel|reels|tv)/([^/?#]+)", url, re.I)
        if m:
            title = _sanitize_text(f"Instagram {m.group(1)}", 500)
    desc = _sanitize_text(_pick_description(info), 8000)
    thumb = _pick_thumb_url(info)
    site = _sanitize_text(str(info.get("extractor") or info.get("ie_key") or ""), 120)

    if not title and not desc and not thumb:
        log.warning(
            "[ytdlp-gateway] empty preview fields keys=%s id=%s",
            list(info.keys())[:35],
            info.get("id"),
        )

    log.info(
        "[ytdlp-gateway] preview ok in %sms url=%s title_len=%s desc_len=%s thumb=%s",
        int((time.perf_counter() - t_req) * 1000),
        url[:120],
        len(title),
        len(desc),
        bool(thumb),
    )

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

# -*- coding: utf-8 -*-
"""
_notion_sync.py — Client Notion + transformation vers le schéma du portail PDJ.
Module partagé importé par les handlers Vercel dans /api/.
Dépendances : stdlib uniquement.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from urllib import error, request

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

# IDs des 10 bases Notion de la page "2026" (pointdujourconseil).
DATABASES: dict[str, str] = {
    "societes":  "343dfc1215cc80d8bca4f624826c626c",
    "projets":   "346dfc1215cc8021897afa6b2cf1ae37",
    "dossiers":  "343dfc1215cc80b0ba49ca34c7d85596",
    "jalons":    "346dfc1215cc80b5bbf8ea550b1127f2",
    "factures":  "343dfc1215cc80469ae7f83e2fa815e5",
    "risques":   "2528f4da0bbb4bfd92f87f0b39a0c758",
    "contacts":  "343dfc1215cc80b0ac5de7c6de8026e9",
    "livrables": "343dfc1215cc80059cadf8bd538b9b99",
    "documents": "343dfc1215cc80d1961bff7b013237c2",
    "reunions":  "343dfc1215cc8036a376f749fecab404",
}

PAGE_URL = "https://www.notion.so/pointdujourconseil/2026-343dfc1215cc806e8babf6920b5a2986"

# ============================================================================
# CLIENT HTTP NOTION
# ============================================================================

class NotionError(Exception):
    pass


def _token() -> str:
    tok = os.environ.get("NOTION_TOKEN", "").strip()
    if not tok:
        raise NotionError(
            "NOTION_TOKEN absent. Définissez-le dans Vercel : "
            "Project Settings → Environment Variables."
        )
    return tok


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_token()}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _http(method: str, url: str, payload: Optional[dict] = None,
          timeout: int = 25) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(url, data=data, method=method, headers=_headers())
    last_err: Optional[Exception] = None
    for attempt in range(4):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            if e.code == 429 or 500 <= e.code < 600:
                time.sleep(min(2 ** attempt, 6))
                last_err = NotionError(f"Notion HTTP {e.code}: {body[:200]}")
                continue
            raise NotionError(f"Notion HTTP {e.code}: {body[:400]}") from e
        except (error.URLError, TimeoutError) as e:
            last_err = NotionError(f"Notion réseau: {e}")
            time.sleep(min(2 ** attempt, 6))
            continue
    raise last_err or NotionError("Notion: échec après retries")


def query_database(database_id: str) -> list[dict]:
    pages: list[dict] = []
    payload: dict[str, Any] = {"page_size": 100}
    while True:
        res = _http("POST", f"{NOTION_API}/databases/{database_id}/query", payload)
        pages.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        payload["start_cursor"] = res["next_cursor"]
    return pages


def retrieve_database(database_id: str) -> dict:
    return _http("GET", f"{NOTION_API}/databases/{database_id}")


# ============================================================================
# EXTRACTEURS PAR TYPE DE PROPRIÉTÉ
# ============================================================================

def _prop(page: dict, name: str) -> Optional[dict]:
    return (page.get("properties") or {}).get(name)


def _plain(rich: list[dict]) -> str:
    return "".join(x.get("plain_text", "") for x in (rich or [])).strip()


def _x_title(p: Optional[dict]) -> Optional[str]:
    if not p or p.get("type") != "title":
        return None
    v = _plain(p.get("title") or [])
    return v or None


def _x_text(p: Optional[dict]) -> Optional[str]:
    if not p:
        return None
    t = p.get("type")
    if t == "rich_text":
        v = _plain(p.get("rich_text") or [])
        return v or None
    if t == "title":
        return _x_title(p)
    if t == "url":
        return p.get("url") or None
    if t == "email":
        return p.get("email") or None
    if t == "phone_number":
        return p.get("phone_number") or None
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "string":
            return (f.get(

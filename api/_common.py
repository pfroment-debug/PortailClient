# -*- coding: utf-8 -*-
"""
_common.py — Helpers partagés entre les handlers Vercel :
  - Authentification par mot de passe (header X-Portal-Password)
  - Cache in-memory par instance lambda (warm reuse)
  - Chargement de curated.json (interpretations + eligibilites)
  - Formatage JSON + gestion CORS
"""

from __future__ import annotations

import hmac
import json
import os
import time
from pathlib import Path
from typing import Any, Optional


# ============================================================================
# AUTHENTIFICATION
# ============================================================================

def portal_password() -> str:
    """Retourne le mot de passe d'accès au portail (vide = libre accès)."""
    return os.environ.get("PORTAL_PASSWORD", "").strip()


def check_auth(headers: dict) -> tuple[bool, str]:
    """Vérifie l'en-tête X-Portal-Password.
    Retourne (autorisé, message)."""
    expected = portal_password()
    if not expected:
        return True, "open"
    provided = ""
    for k, v in headers.items():
        if k.lower() == "x-portal-password":
            provided = (v or "").strip()
            break
    if provided and hmac.compare_digest(provided, expected):
        return True, "ok"
    return False, "unauthorized"


# ============================================================================
# CACHE IN-MEMORY PAR INSTANCE LAMBDA
# ============================================================================

_CACHE: dict[str, Any] = {"data": None, "ts": 0.0, "error": None}
DEFAULT_TTL_S = int(os.environ.get("PDJ_CACHE_TTL_S", "300"))


def cache_get() -> tuple[Optional[dict], float, Optional[str]]:
    return _CACHE["data"], _CACHE["ts"], _CACHE["error"]


def cache_set(data: Optional[dict], err: Optional[str] = None) -> None:
    _CACHE["data"] = data
    _CACHE["ts"] = time.time()
    _CACHE["error"] = err


def cache_is_fresh(ttl_s: int = DEFAULT_TTL_S) -> bool:
    if _CACHE["data"] is None:
        return False
    return (time.time() - _CACHE["ts"]) < ttl_s


def cache_age_s() -> Optional[float]:
    if _CACHE["ts"] == 0:
        return None
    return round(time.time() - _CACHE["ts"], 1)


# ============================================================================
# CURATED (contenu rédactionnel non issu de Notion)
# ============================================================================

def load_curated() -> dict:
    """Charge curated.json. Renvoie {} si absent ou invalide."""
    candidates = [
        Path(__file__).parent.parent / "curated.json",
        Path(__file__).parent / "curated.json",
        Path.cwd() / "curated.json",
    ]
    for p in candidates:
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"[curated] erreur lecture {p}: {e}", flush=True)
                return {}
    return {}


# ============================================================================
# RÉPONSES HTTP JSON
# ============================================================================

def json_response(handler, status: int, payload: dict,
                  cache_control: str = "no-store") -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", cache_control)
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Headers",
                        "Content-Type, X-Portal-Password")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.end_headers()
    handler.wfile.write(body)


def json_error(handler, status: int, message: str) -> None:
    json_response(handler, status, {"ok": False, "error": message})


def handle_options(handler) -> None:
    """Répondre aux pré-vols CORS."""
    handler.send_response(204)
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Headers",
                        "Content-Type, X-Portal-Password")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Max-Age", "86400")
    handler.end_headers()

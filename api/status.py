# -*- coding: utf-8 -*-
"""GET /api/status → diagnostic rapide (utilisé par le front pour détecter
le mode LIVE)."""

from http.server import BaseHTTPRequestHandler
import os
from _common import (cache_age_s, cache_get, handle_options, json_response,
                     portal_password)


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        # /api/status est public même si un mot de passe est défini
        # (le front appelle cet endpoint avant d'afficher l'écran de login)
        data, ts, err = cache_get()
        json_response(self, 200, {
            "ok": True,
            "ai_available": bool(os.environ.get("ANTHROPIC_API_KEY", "").strip()),
            "notion_configured": bool(os.environ.get("NOTION_TOKEN", "").strip()),
            "auth_required": bool(portal_password()),
            "cache_age_s": cache_age_s(),
            "cache_populated": data is not None,
            "last_error": err,
        })

    def log_message(self, format, *args):
        return

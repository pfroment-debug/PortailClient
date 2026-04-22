# -*- coding: utf-8 -*-
"""GET /api/data → JSON complet du portail (Notion + curated)."""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from http.server import BaseHTTPRequestHandler
from _common import (cache_get, cache_is_fresh, cache_set, check_auth,
                     handle_options, json_error, json_response, load_curated)
from _notion_sync import NotionError, sync_all


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        ok, reason = check_auth(dict(self.headers))
        if not ok:
            return json_error(self, 401, "Mot de passe portail requis.")

        if cache_is_fresh():
            data, _, _ = cache_get()
            return json_response(self, 200, data,
                                 cache_control="public, s-maxage=60")

        try:
            curated = load_curated()
            data = sync_all(curated=curated)
            cache_set(data)
            return json_response(self, 200, data,
                                 cache_control="public, s-maxage=60")
        except NotionError as e:
            cache_set(None, err=str(e))
            return json_error(self, 502, f"Notion: {e}")
        except Exception as e:
            cache_set(None, err=str(e))
            return json_error(self, 500, f"Erreur sync: {e}")

    def log_message(self, format, *args):
        return

# -*- coding: utf-8 -*-
"""POST /api/refresh → force une resynchronisation Notion (ignore le cache)."""

from http.server import BaseHTTPRequestHandler
from _common import (cache_set, check_auth, handle_options, json_error,
                     json_response, load_curated)
from _notion_sync import NotionError, sync_all


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_POST(self):
        ok, _ = check_auth(dict(self.headers))
        if not ok:
            return json_error(self, 401, "Mot de passe portail requis.")

        try:
            data = sync_all(curated=load_curated())
            cache_set(data)
            return json_response(self, 200,
                                 {"ok": True,
                                  "counts": data.get("_meta", {}).get("counts"),
                                  "duration_s": data.get("_meta", {}).get("sync_duration_s")})
        except NotionError as e:
            cache_set(None, err=str(e))
            return json_error(self, 502, f"Notion: {e}")
        except Exception as e:
            cache_set(None, err=str(e))
            return json_error(self, 500, f"Erreur sync: {e}")

    def log_message(self, format, *args):
        return

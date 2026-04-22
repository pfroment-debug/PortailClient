# -*- coding: utf-8 -*-
"""GET /api/inspect → liste les propriétés réelles de chaque base Notion.
Utile pour adapter les noms dans _notion_sync.NAMES si votre workspace en
utilise d'autres."""

from http.server import BaseHTTPRequestHandler
from _common import check_auth, handle_options, json_error, json_response
from _notion_sync import NotionError, inspect_schemas


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        ok, _ = check_auth(dict(self.headers))
        if not ok:
            return json_error(self, 401, "Mot de passe portail requis.")

        try:
            schemas = inspect_schemas()
            return json_response(self, 200, {"ok": True, "schemas": schemas})
        except NotionError as e:
            return json_error(self, 502, f"Notion: {e}")
        except Exception as e:
            return json_error(self, 500, f"Erreur: {e}")

    def log_message(self, format, *args):
        return

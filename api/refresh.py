# -*- coding: utf-8 -*-
"""POST /api/analyze → proxy sécurisé vers Claude (Anthropic API).

Le front envoie { mode, entity_key, lecture }, le serveur construit le
contexte scopé puis appelle Claude. Le token Anthropic reste côté serveur.
"""

import json
import os
from http.server import BaseHTTPRequestHandler
from urllib import error, request

from _common import (cache_get, cache_is_fresh, cache_set, check_auth,
                     handle_options, json_error, json_response, load_curated)
from _notion_sync import NotionError, sync_all

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-5"

SYSTEM_PROMPTS = {
    "t1": ("Tu es consultant fiscal senior de Point du Jour Conseil (PDJ), "
           "cabinet spécialisé en CIR, CII, JEI. Tu t'adresses à un DAF ou un "
           "RAF. Ton registre : rigueur fiscale, maîtrise du FRASCATI, lecture "
           "précise de la chaîne dépenses engagées → valorisables → montant "
           "CI. Produis une analyse structurée (200-350 mots) avec sections "
           "courtes : (1) situation fiscale en chiffres, (2) 2-3 points "
           "d'attention concrets, (3) 2-3 actions avant dépôt, (4) angle mort "
           "éventuel. Utilise le markdown."),
    "t2": ("Tu es directeur scientifique R&D senior, habitué à lire un "
           "portefeuille selon la méthode 'Densification 3 Temps' de PDJ. Tu "
           "t'adresses à la Direction R&D. Ton registre : rigueur scientifique, "
           "lecture des verrous, TRL actuel vs cible, cohérence des axes, "
           "gouvernance, trajectoire pluriannuelle. Produis une analyse (250-"
           "400 mots) : (1) cohérence du portefeuille, (2) axes thématiques et "
           "trajectoire, (3) verrous critiques, (4) répartition gouvernance/"
           "risque, (5) 2-3 leviers pour densifier le dossier technique CIR. "
           "Markdown."),
    "t3": ("Tu es consultant stratégique senior PDJ, rompu au dialogue avec "
           "comités de direction. Tu t'adresses à la DG. Ton registre : "
           "hauteur institutionnelle, capital immatériel, positionnement, "
           "risques réputationnels, trajectoire pluriannuelle, doctrine du "
           "'Reciblage Stratégique'. Produis une analyse (300-450 mots) : "
           "(1) diagnostic institutionnel, (2) 1-2 questionnements stratégiques "
           "majeurs (PI, M&A, IP Box, souveraineté…), (3) trajectoire "
           "(momentum, inflexion, risque de dispersion), (4) 2 options "
           "stratégiques actionnables par le CODIR, (5) alertes = enjeux de "
           "crédibilité institutionnelle. Markdown."),
}

MODE_LABELS = {
    "client":  "Vue d'ensemble du client",
    "projet":  "Focus projet R&D",
    "year":    "Focus exercice",
    "dossier": "Focus dossier fiscal",
}


def _build_context(data: dict, mode: str, entity_key: str) -> dict:
    if mode == "client":
        s = next((x for x in data["societes"] if x["nom"] == entity_key), None)
        if not s:
            return {}
        return {
            "societe":        s,
            "projets":        [p for p in data["projets"]   if p["societe"] == entity_key],
            "jalons":         [j for j in data["jalons"]    if j["societe"] == entity_key],
            "dossiers":       [d for d in data["dossiers"]  if d["societe"] == entity_key],
            "factures":       [f for f in data["factures"]  if f["societe"] == entity_key],
            "risques":        [r for r in data.get("risques", []) if r["societe"] == entity_key],
            "interpretation": (data.get("interpretations") or {}).get(entity_key),
        }
    if mode == "projet":
        p = next((x for x in data["projets"] if x["id"] == entity_key), None)
        if not p:
            return {}
        key = p["nom"].split(" — ")[0]
        return {"projet": p,
                "jalons": [j for j in data["jalons"]
                           if j["societe"] == p["societe"]
                           and (key in j["projet"] or j["projet"] in p["nom"])]}
    if mode == "year":
        try:
            soc, y = entity_key.split(":")
            year = int(y)
        except Exception:
            return {}
        return {
            "societe":  soc, "annee": year,
            "jalons":   [j for j in data["jalons"]   if j["societe"] == soc and j["annee"] == year],
            "projets":  [p for p in data["projets"]  if p["societe"] == soc
                         and (p.get("demarrage") or 0) <= year
                         and (p.get("cloture")   or 9999) >= year],
            "dossiers": [d for d in data["dossiers"] if d["societe"] == soc and d["annee"] == year],
            "factures": [f for f in data["factures"] if f["societe"] == soc and f["exercice"] == year],
            "risques":  [r for r in data.get("risques", []) if r["societe"] == soc
                         and (r.get("date_evenement") or "").startswith(str(year))],
        }
    if mode == "dossier":
        d = next((x for x in data["dossiers"] if x["id"] == entity_key), None)
        if not d:
            return {}
        return {"dossier": d,
                "jalons": [j for j in data["jalons"]
                           if j["societe"] == d["societe"]
                           and j["annee"] == d["annee"]
                           and (d["type"] == j.get("type_ci")
                                or d["type"] in ("AGR", "Audit", "Autre"))]}
    return {}


def _call_anthropic(api_key: str, system: str, user_msg: str) -> str:
    body = json.dumps({
        "model": MODEL,
        "max_tokens": 2000,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }).encode("utf-8")
    req = request.Request(ANTHROPIC_API, data=body, method="POST", headers={
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    })
    try:
        with request.urlopen(req, timeout=50) as resp:
            out = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        txt = ""
        try:
            txt = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"Anthropic HTTP {e.code}: {txt[:300]}") from e
    return "".join(b.get("text", "") for b in out.get("content", [])
                   if b.get("type") == "text")


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_POST(self):
        ok, _ = check_auth(dict(self.headers))
        if not ok:
            return json_error(self, 401, "Mot de passe portail requis.")

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return json_error(self, 503, "ANTHROPIC_API_KEY non configurée "
                                         "côté serveur.")

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as e:
            return json_error(self, 400, f"JSON body invalide: {e}")

        mode = body.get("mode", "client")
        entity_key = body.get("entity_key", "")
        lecture = body.get("lecture", "t1")
        if lecture not in SYSTEM_PROMPTS:
            return json_error(self, 400, f"lecture inconnue: {lecture}")

        if not cache_is_fresh():
            try:
                data = sync_all(curated=load_curated())
                cache_set(data)
            except Exception as e:
                return json_error(self, 502, f"Sync Notion: {e}")
        data, _, _ = cache_get()

        ctx = _build_context(data, mode, entity_key)
        if not ctx:
            return json_error(self, 404,
                              f"Contexte introuvable pour {mode}:{entity_key}")

        user_msg = (
            f"## Vue : {MODE_LABELS.get(mode, mode)}\n\n"
            f"## Données scopées (extrait JSON)\n\n```json\n"
            f"{json.dumps(ctx, ensure_ascii=False, indent=2)}\n```\n\n"
            "Analyse ces données selon ton rôle. Appuie-toi uniquement sur "
            "les chiffres et champs présents. Ne fabrique pas de données "
            "absentes."
        )

        try:
            text = _call_anthropic(api_key, SYSTEM_PROMPTS[lecture], user_msg)
        except Exception as e:
            return json_error(self, 502, str(e))

        return json_response(self, 200, {
            "ok": True, "analysis": text, "model": MODEL,
        })

    def log_message(self, format, *args):
        return

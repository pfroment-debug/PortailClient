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
            "jalons":   [j for j in data["jalons"]   if j["societe"] == soc and j["anne

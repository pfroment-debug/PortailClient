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
            return (f.get("string") or "").strip() or None
        if f.get("type") == "number":
            n = f.get("number")
            return None if n is None else str(n)
    return None


def _x_select(p: Optional[dict]) -> Optional[str]:
    if not p:
        return None
    t = p.get("type")
    if t == "select":
        s = p.get("select") or {}
        return s.get("name") or None
    if t == "status":
        s = p.get("status") or {}
        return s.get("name") or None
    if t == "multi_select":
        items = p.get("multi_select") or []
        return items[0].get("name") if items else None
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "string":
            return (f.get("string") or "").strip() or None
    return None


def _x_multi(p: Optional[dict]) -> list[str]:
    if not p:
        return []
    if p.get("type") == "multi_select":
        return [x.get("name") for x in (p.get("multi_select") or []) if x.get("name")]
    if p.get("type") == "select":
        s = p.get("select") or {}
        return [s["name"]] if s.get("name") else []
    return []


def _x_number(p: Optional[dict]) -> Optional[float]:
    if not p:
        return None
    t = p.get("type")
    if t == "number":
        return p.get("number")
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "number":
            return f.get("number")
        if f.get("type") == "string":
            try:
                return float((f.get("string") or "").replace(",", "."))
            except Exception:
                return None
    if t == "rollup":
        r = p.get("rollup") or {}
        if r.get("type") == "number":
            return r.get("number")
    return None


def _x_date(p: Optional[dict]) -> Optional[str]:
    if not p:
        return None
    t = p.get("type")
    if t == "date":
        d = p.get("date") or {}
        return (d.get("start") or None)
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "date":
            d = f.get("date") or {}
            return (d.get("start") or None)
    return None


def _x_checkbox(p: Optional[dict]) -> Optional[bool]:
    if not p:
        return None
    if p.get("type") == "checkbox":
        return bool(p.get("checkbox"))
    return None


def _x_relation_ids(p: Optional[dict]) -> list[str]:
    if not p or p.get("type") != "relation":
        return []
    return [r["id"] for r in (p.get("relation") or []) if r.get("id")]


# ============================================================================
# HELPERS : ESSAYER PLUSIEURS NOMS DE PROPRIÉTÉS
# ============================================================================

def _try(page: dict, names: list[str],
         extractor: Callable[[Optional[dict]], Any]) -> Any:
    for name in names:
        p = _prop(page, name)
        if p is None:
            continue
        v = extractor(p)
        if v not in (None, "", []):
            return v
    return None


def _try_relation(page: dict, names: list[str]) -> list[str]:
    for name in names:
        p = _prop(page, name)
        if p is None:
            continue
        v = _x_relation_ids(p)
        if v:
            return v
    return []


# ============================================================================
# MAPPING PAR BASE — noms candidats pour chaque champ cible
# ============================================================================

NAMES = {
    # SOCIÉTÉS
    "societe.nom":    ["Nom", "Name", "Société", "Societe"],
    "societe.statut": ["Statut", "Status", "Type"],
    "societe.lieu":   ["Lieu", "Ville", "Adresse", "Location", "Localisation"],

    # PROJETS
    "projet.nom":          ["Nom", "Name", "Projet", "Titre"],
    "projet.societe":      ["Société", "Societe", "Client", "Entreprise"],
    "projet.objectif":     ["Objectif", "Objectifs", "Description", "But"],
    "projet.verrous":      ["Verrous", "Verrou", "Verrou scientifique",
                            "Verrous scientifiques", "Difficulté"],
    "projet.trl":          ["TRL", "TRL actuel", "TRL Actuel", "TRL initial"],
    "projet.trl_cible":    ["TRL Cible", "TRL cible", "TRL_cible", "TRL target"],
    "projet.gouvernance":  ["Gouvernance", "Mode", "Type de projet"],
    "projet.strategie_pi": ["Stratégie PI", "Strategie PI", "Dispositif",
                            "Stratégie", "Strat PI", "Type"],
    "projet.demarrage":    ["Démarrage", "Demarrage", "Début", "Année début",
                            "Start"],
    "projet.cloture":      ["Clôture", "Cloture", "Fin", "Année fin", "End"],
    "projet.score_r":      ["Score R", "Score_R", "R"],
    "projet.score_i":      ["Score I", "Score_I", "I"],
    "projet.score_d":      ["Score D", "Score_D", "D"],
    "projet.axe_rdi":      ["Axe R&D&I", "Axe", "Axe RDI", "Axe RD&I"],
    "projet.risque":       ["Risque", "Alerte", "Niveau risque"],

    # DOSSIERS
    "dossier.nom":     ["Nom", "Name", "Dossier", "Titre"],
    "dossier.societe": ["Société", "Societe", "Client"],
    "dossier.type":    ["Type", "Type de dossier", "Dispositif"],
    "dossier.annee":   ["Année", "Annee", "Exercice", "Year"],

    # JALONS
    "jalon.projet":               ["Projet", "Project", "Nom du projet"],
    "jalon.societe":              ["Société", "Societe", "Client"],
    "jalon.annee":                ["Année", "Annee", "Exercice", "Year"],
    "jalon.type_ci":              ["Type CI", "Type", "Dispositif", "Type de CI"],
    "jalon.depenses_engagees":    ["Dépenses engagées", "Depenses engagees",
                                   "Engagées", "Engagees", "Dép engagées"],
    "jalon.depenses_valorisables":["Dépenses valorisables", "Depenses valorisables",
                                   "Valorisables", "Dép valo"],
    "jalon.montant_ci":           ["Montant CI", "Montant", "CI", "CI obtenu"],
    "jalon.avancement":           ["Avancement", "Progress", "% avancement",
                                   "Progression"],
    "jalon.libelle_subvention":   ["Libellé subvention", "Libelle subvention",
                                   "Subvention", "Nom subvention"],
    "jalon.subvention_percue":    ["Subvention perçue", "Subvention percue",
                                   "Montant subvention", "Subv perçue"],
    "jalon.note_fiscale":         ["Note fiscale", "Note", "Notes fiscales"],
    "jalon.alerte":               ["Alerte", "Risque", "Severite"],

    # FACTURES
    "facture.nom":       ["Nom", "Name", "Intitulé", "Intitule", "Référence"],
    "facture.societe":   ["Société", "Societe", "Client"],
    "facture.type":      ["Type", "Nature"],
    "facture.etat":      ["État", "Etat", "Status", "Statut"],
    "facture.montant":   ["Montant", "Amount", "Prix HT", "Total HT"],
    "facture.exercice":  ["Exercice", "Année", "Annee", "Year"],
    "facture.date":      ["Date", "Date facture", "Date d'émission"],

    # RISQUES
    "risque.nom":             ["Nom", "Name", "Alerte", "Titre"],
    "risque.societe":         ["Société", "Societe", "Client"],
    "risque.type_alerte":     ["Type d'alerte", "Type alerte", "Type",
                               "Catégorie"],
    "risque.severite":        ["Sévérité", "Severite", "Niveau", "Priority"],
    "risque.statut":          ["Statut", "Status", "État", "Etat"],
    "risque.montant_expose":  ["Montant exposé", "Montant expose", "Exposition",
                               "Montant", "Enjeu"],
    "risque.date_evenement":  ["Date événement", "Date evenement", "Date",
                               "Date de survenue"],
    "risque.date_limite":     ["Date limite", "Deadline", "Échéance", "Echeance"],
    "risque.actions":         ["Actions", "Actions à mener", "Plan d'action",
                               "Recommandations"],
    "risque.projets_lies":    ["Projets liés", "Projets lies", "Projet",
                               "Projets"],

    # CONTACTS
    "contact.prenom":      ["Prénom", "Prenom", "First name", "First"],
    "contact.nom_famille": ["Nom", "Name", "Nom de famille", "Last name"],
    "contact.email":       ["Email", "Mail", "E-mail", "Courriel"],
    "contact.phone":       ["Téléphone", "Telephone", "Phone", "Tél", "Tel"],
    "contact.fonction":    ["Fonction", "Role", "Rôle", "Poste", "Title"],
    "contact.societe":     ["Société", "Societe", "Client", "Entreprise"],

    # LIVRABLES
    "livrable.nom":         ["Nom", "Name", "Livrable", "Titre"],
    "livrable.societe":     ["Société", "Societe", "Client"],
    "livrable.type":        ["Type", "Nature"],
    "livrable.etat":        ["État", "Etat", "Status", "Statut"],
    "livrable.priorite":    ["Priorité", "Priorite", "Priority"],
    "livrable.deadline":    ["Deadline", "Échéance", "Echeance", "Date limite",
                             "Date"],
    "livrable.projets_lies":["Projets liés", "Projets lies", "Projet",
                             "Projets"],
    "livrable.dossiers":    ["Dossiers", "Dossier", "Dossiers liés"],

    # DOCUMENTS
    "document.nom":         ["Nom", "Name", "Titre", "Document"],
    "document.societe":     ["Société", "Societe", "Client"],
    "document.type":        ["Type", "Nature", "Format"],
    "document.url":         ["URL", "Url", "Lien", "Link"],
    "document.dossiers":    ["Dossiers", "Dossier", "Dossiers liés"],
    "document.livrables":   ["Livrables", "Livrable", "Livrables liés"],

    # RÉUNIONS
    "reunion.nom":           ["Nom", "Name", "Titre", "Réunion"],
    "reunion.societe":       ["Société", "Societe", "Client"],
    "reunion.type":          ["Type", "Nature", "Catégorie"],
    "reunion.date":          ["Date", "Date réunion", "Date meeting"],
    "reunion.projets_lies":  ["Projets liés", "Projets lies", "Projet",
                              "Projets"],
    "reunion.livrables":     ["Livrables", "Livrable"],
}


# ============================================================================
# TRANSFORMERS — une page Notion → un dict conforme au schéma du portail
# ============================================================================

def transform_societe(page: dict) -> dict:
    return {
        "id":     page["id"].replace("-", ""),
        "nom":    _try(page, NAMES["societe.nom"], _x_title) or "",
        "statut": _try(page, NAMES["societe.statut"], _x_select) or "Client",
        "lieu":   _try(page, NAMES["societe.lieu"], _x_text) or "",
    }


def _resolve_societe_name(page: dict, societe_by_id: dict[str, str],
                          names_candidates: list[str]) -> str:
    ids = _try_relation(page, names_candidates)
    for i in ids:
        key = i.replace("-", "")
        if key in societe_by_id:
            return societe_by_id[key]
    t = _try(page, names_candidates, _x_text)
    return t or ""


def _resolve_names_for_ids(ids: list[str], name_by_id: dict[str, str]) -> list[str]:
    out = []
    for i in ids:
        key = i.replace("-", "")
        if key in name_by_id:
            out.append(name_by_id[key])
    return out


def transform_projet(page: dict, societe_by_id: dict[str, str]) -> dict:
    score_r = _try(page, NAMES["projet.score_r"], _x_number) or 0
    score_i = _try(page, NAMES["projet.score_i"], _x_number) or 0
    score_d = _try(page, NAMES["projet.score_d"], _x_number) or 0
    axe = _try(page, NAMES["projet.axe_rdi"], _x_select) or \
          _try(page, NAMES["projet.axe_rdi"], _x_text)
    if not axe:
        parts = []
        if score_r: parts.append("R")
        if score_d: parts.append("D")
        if score_i: parts.append("I")
        axe = "&".join(parts) or "—"

    return {
        "id":           page["id"].replace("-", ""),
        "societe":      _resolve_societe_name(page, societe_by_id,
                                              NAMES["projet.societe"]),
        "nom":          _try(page, NAMES["projet.nom"], _x_title) or "",
        "objectif":     _try(page, NAMES["projet.objectif"], _x_text) or "",
        "verrous":      _try(page, NAMES["projet.verrous"], _x_text) or "",
        "trl":          int(_try(page, NAMES["projet.trl"], _x_number) or 0),
        "trl_cible":    int(_try(page, NAMES["projet.trl_cible"], _x_number) or 0),
        "gouvernance":  _try(page, NAMES["projet.gouvernance"], _x_select)
                        or "Interne",
        "strategie_pi": _try(page, NAMES["projet.strategie_pi"], _x_select)
                        or "N.A",
        "demarrage":    int(_try(page, NAMES["projet.demarrage"], _x_number) or 0)
                        or None,
        "cloture":      int(_try(page, NAMES["projet.cloture"], _x_number) or 0)
                        or None,
        "score_r":      int(score_r),
        "score_i":      int(score_i),
        "score_d":      int(score_d),
        "axe_rdi":      axe,
        "risque":       _try(page, NAMES["projet.risque"], _x_select),
    }


def transform_dossier(page: dict, societe_by_id: dict[str, str]) -> dict:
    return {
        "id":      page["id"].replace("-", ""),
        "nom":     _try(page, NAMES["dossier.nom"], _x_title) or "",
        "societe": _resolve_societe_name(page, societe_by_id,
                                         NAMES["dossier.societe"]),
        "type":    _try(page, NAMES["dossier.type"], _x_select) or "CIR",
        "annee":   int(_try(page, NAMES["dossier.annee"], _x_number) or 0) or None,
    }


def transform_jalon(page: dict, societe_by_id: dict[str, str],
                    projet_by_id: dict[str, str]) -> dict:
    proj_ids = _try_relation(page, NAMES["jalon.projet"])
    projet_nom = ""
    if proj_ids:
        for i in proj_ids:
            k = i.replace("-", "")
            if k in projet_by_id:
                projet_nom = projet_by_id[k]
                break
    if not projet_nom:
        projet_nom = _try(page, NAMES["jalon.projet"], _x_text) or ""

    av = _try(page, NAMES["jalon.avancement"], _x_number)
    if av is not None and av > 1.5:
        av = av / 100.0

    out = {
        "projet":                projet_nom,
        "societe":               _resolve_societe_name(page, societe_by_id,
                                                       NAMES["jalon.societe"]),
        "annee":                 int(_try(page, NAMES["jalon.annee"],
                                          _x_number) or 0) or None,
        "type_ci":               _try(page, NAMES["jalon.type_ci"],
                                      _x_select) or "",
        "depenses_engagees":     _try(page, NAMES["jalon.depenses_engagees"],
                                      _x_number) or 0,
        "depenses_valorisables": _try(page, NAMES["jalon.depenses_valorisables"],
                                      _x_number) or 0,
        "montant_ci":            _try(page, NAMES["jalon.montant_ci"],
                                      _x_number) or 0,
        "avancement":            av,
    }
    lbl = _try(page, NAMES["jalon.libelle_subvention"], _x_text)
    if lbl: out["libelle_subvention"] = lbl
    sv = _try(page, NAMES["jalon.subvention_percue"], _x_number)
    if sv: out["subvention_percue"] = sv
    note = _try(page, NAMES["jalon.note_fiscale"], _x_text)
    if note: out["note_fiscale"] = note
    alerte = _try(page, NAMES["jalon.alerte"], _x_select)
    if alerte: out["alerte"] = alerte.lower()
    return out


def transform_facture(page: dict, societe_by_id: dict[str, str]) -> dict:
    return {
        "nom":      _try(page, NAMES["facture.nom"], _x_title) or "",
        "societe":  _resolve_societe_name(page, societe_by_id,
                                          NAMES["facture.societe"]),
        "type":     _try(page, NAMES["facture.type"], _x_select) or "Autre",
        "etat":     _try(page, NAMES["facture.etat"], _x_select) or "A facturer",
        "montant":  _try(page, NAMES["facture.montant"], _x_number) or 0,
        "exercice": int(_try(page, NAMES["facture.exercice"], _x_number) or 0)
                    or None,
        "date":     _try(page, NAMES["facture.date"], _x_date),
    }


def transform_risque(page: dict, societe_by_id: dict[str, str],
                     projet_by_id: dict[str, str]) -> dict:
    proj_ids = _try_relation(page, NAMES["risque.projets_lies"])
    return {
        "id":             page["id"].replace("-", ""),
        "nom":            _try(page, NAMES["risque.nom"], _x_title) or "",
        "societe":        _resolve_societe_name(page, societe_by_id,
                                                NAMES["risque.societe"]),
        "type_alerte":    _try(page, NAMES["risque.type_alerte"], _x_select)
                          or "",
        "severite":       _try(page, NAMES["risque.severite"], _x_select)
                          or "Info",
        "statut":         _try(page, NAMES["risque.statut"], _x_select)
                          or "À traiter",
        "montant_expose": _try(page, NAMES["risque.montant_expose"], _x_number)
                          or 0,
        "date_evenement": _try(page, NAMES["risque.date_evenement"], _x_date),
        "date_limite":    _try(page, NAMES["risque.date_limite"], _x_date),
        "actions":        _try(page, NAMES["risque.actions"], _x_text) or "",
        "projets_lies":   _resolve_names_for_ids(proj_ids, projet_by_id),
    }


def transform_contact(page: dict, societe_by_id: dict[str, str]) -> dict:
    return {
        "id":          page["id"].replace("-", ""),
        "prenom":      _try(page, NAMES["contact.prenom"], _x_text) or "",
        "nom_famille": _try(page, NAMES["contact.nom_famille"], _x_title)
                       or _try(page, NAMES["contact.nom_famille"], _x_text)
                       or "",
        "email":       _try(page, NAMES["contact.email"], _x_text) or "",
        "phone":       _try(page, NAMES["contact.phone"], _x_text) or "",
        "fonction":    _try(page, NAMES["contact.fonction"], _x_select)
                       or _try(page, NAMES["contact.fonction"], _x_text) or "",
        "societe":     _resolve_societe_name(page, societe_by_id,
                                             NAMES["contact.societe"]),
    }


def transform_livrable(page: dict, societe_by_id: dict[str, str],
                       projet_by_id: dict[str, str],
                       dossier_by_id: dict[str, str]) -> dict:
    proj_ids = _try_relation(page, NAMES["livrable.projets_lies"])
    doss_ids = _try_relation(page, NAMES["livrable.dossiers"])
    return {
        "id":           page["id"].replace("-", ""),
        "nom":          _try(page, NAMES["livrable.nom"], _x_title) or "",
        "societe":      _resolve_societe_name(page, societe_by_id,
                                              NAMES["livrable.societe"]),
        "type":         _try(page, NAMES["livrable.type"], _x_select) or "",
        "etat":         _try(page, NAMES["livrable.etat"], _x_select)
                        or "En cours",
        "priorite":     _try(page, NAMES["livrable.priorite"], _x_select)
                        or "Moyenne",
        "deadline":     _try(page, NAMES["livrable.deadline"], _x_date),
        "projets_lies": _resolve_names_for_ids(proj_ids, projet_by_id),
        "dossiers_ids": [i.replace("-", "") for i in doss_ids],
    }


def transform_document(page: dict, societe_by_id: dict[str, str],
                       dossier_by_id: dict[str, str],
                       livrable_by_id: dict[str, str]) -> dict:
    doss_ids = _try_relation(page, NAMES["document.dossiers"])
    liv_ids = _try_relation(page, NAMES["document.livrables"])
    return {
        "id":            page["id"].replace("-", ""),
        "nom":           _try(page, NAMES["document.nom"], _x_title) or "",
        "societe":       _resolve_societe_name(page, societe_by_id,
                                               NAMES["document.societe"]),
        "type":          _try(page, NAMES["document.type"], _x_select) or "Autre",
        "url":           _try(page, NAMES["document.url"], _x_text) or "",
        "dossiers_ids":  [i.replace("-", "") for i in doss_ids],
        "livrables_ids": [i.replace("-", "") for i in liv_ids],
    }


def transform_reunion(page: dict, societe_by_id: dict[str, str],
                      projet_by_id: dict[str, str],
                      livrable_by_id: dict[str, str]) -> dict:
    proj_ids = _try_relation(page, NAMES["reunion.projets_lies"])
    liv_ids  = _try_relation(page, NAMES["reunion.livrables"])
    return {
        "id":            page["id"].replace("-", ""),
        "nom":           _try(page, NAMES["reunion.nom"], _x_title) or "",
        "societe":       _resolve_societe_name(page, societe_by_id,
                                               NAMES["reunion.societe"]),
        "type":          _try(page, NAMES["reunion.type"], _x_select) or "Autre",
        "date":          _try(page, NAMES["reunion.date"], _x_date),
        "projets_lies":  _resolve_names_for_ids(proj_ids, projet_by_id),
        "livrables_ids": [i.replace("-", "") for i in liv_ids],
    }


# ============================================================================
# ORCHESTRATEUR
# ============================================================================

def _fetch_all_parallel() -> dict[str, list[dict]]:
    raw: dict[str, list[dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(query_database, dbid): key
                   for key, dbid in DATABASES.items()}
        for fut in concurrent.futures.as_completed(futures):
            key = futures[fut]
            try:
                raw[key] = fut.result()
            except Exception as e:
                raw[key] = []
                print(f"[sync] erreur fetch {key}: {e}", flush=True)
    return raw


def sync_all(curated: Optional[dict] = None) -> dict:
    t0 = time.time()
    raw = _fetch_all_parallel()

    societes = [transform_societe(p) for p in raw.get("societes", [])]
    societe_by_id = {s["id"]: s["nom"] for s in societes}

    projets = [transform_projet(p, societe_by_id)
               for p in raw.get("projets", [])]
    projet_by_id = {p["id"]: p["nom"] for p in projets}

    dossiers = [transform_dossier(p, societe_by_id)
                for p in raw.get("dossiers", [])]
    dossier_by_id = {d["id"]: d["nom"] for d in dossiers}

    jalons = [transform_jalon(p, societe_by_id, projet_by_id)
              for p in raw.get("jalons", [])]
    factures = [transform_facture(p, societe_by_id)
                for p in raw.get("factures", [])]
    risques = [transform_risque(p, societe_by_id, projet_by_id)
               for p in raw.get("risques", [])]
    contacts = [transform_contact(p, societe_by_id)
                for p in raw.get("contacts", [])]

    livrables = [transform_livrable(p, societe_by_id, projet_by_id, dossier_by_id)
                 for p in raw.get("livrables", [])]
    livrable_by_id = {l["id"]: l["nom"] for l in livrables}

    documents = [transform_document(p, societe_by_id, dossier_by_id,
                                    livrable_by_id)
                 for p in raw.get("documents", [])]
    reunions = [transform_reunion(p, societe_by_id, projet_by_id, livrable_by_id)
                for p in raw.get("reunions", [])]

    result = {
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "Notion — Page 2026 (pointdujourconseil)",
            "page_url": PAGE_URL,
            "databases": DATABASES,
            "counts": {
                "societes":  len(societes),
                "projets":   len(projets),
                "dossiers":  len(dossiers),
                "jalons":    len(jalons),
                "factures":  len(factures),
                "risques":   len(risques),
                "contacts":  len(contacts),
                "livrables": len(livrables),
                "documents": len(documents),
                "reunions":  len(reunions),
            },
            "sync_duration_s": round(time.time() - t0, 2),
        },
        "societes":  societes,
        "projets":   projets,
        "dossiers":  dossiers,
        "jalons":    jalons,
        "factures":  factures,
        "risques":   risques,
        "contacts":  contacts,
        "livrables": livrables,
        "documents": documents,
        "reunions":  reunions,
    }

    if curated:
        result["interpretations"] = curated.get("interpretations", {})
        result["eligibilites"]    = curated.get("eligibilites", {})

    return result


def inspect_schemas() -> dict:
    out: dict[str, Any] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(retrieve_database, dbid): key
                for key, dbid in DATABASES.items()}
        for fut in concurrent.futures.as_completed(futs):
            key = futs[fut]
            try:
                meta = fut.result()
                props = meta.get("properties", {}) or {}
                out[key] = [
                    {"name": name, "type": info.get("type")}
                    for name, info in sorted(props.items())
                ]
            except Exception as e:
                out[key] = {"error": str(e)}
    return out

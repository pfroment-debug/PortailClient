# -*- coding: utf-8 -*-
"""_notion_sync.py — Client Notion + transformation adaptée au workspace PDJ.

Logique métier clé : un jalon peut être rattaché à PLUSIEURS dossiers
(ex. un même jalon CIR figure dans le dossier CIR pour le calcul CI, ET
dans un dossier Subvention pour tracer l'aide perçue à déduire).
La relation jalon → dossiers est donc exposée en liste (dossiers_ids).
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib import error, request

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

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


class NotionError(Exception):
    pass


def _token() -> str:
    tok = os.environ.get("NOTION_TOKEN", "").strip()
    if not tok:
        raise NotionError("NOTION_TOKEN absent dans les env vars Vercel.")
    return tok


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_token()}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _http(method: str, url: str, payload: Optional[dict] = None, timeout: int = 25) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(url, data=data, method=method, headers=_headers())
    last_err: Optional[Exception] = None
    for attempt in range(4):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as e:
            body = ""
            try: body = e.read().decode("utf-8", errors="replace")
            except Exception: pass
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
    pages, payload = [], {"page_size": 100}
    while True:
        res = _http("POST", f"{NOTION_API}/databases/{database_id}/query", payload)
        pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        payload["start_cursor"] = res["next_cursor"]
    return pages


def retrieve_database(database_id: str) -> dict:
    return _http("GET", f"{NOTION_API}/databases/{database_id}")


# ======== EXTRACTEURS ========

def _prop(page, name):
    return (page.get("properties") or {}).get(name)

def _plain(rich):
    return "".join(x.get("plain_text", "") for x in (rich or [])).strip()

def _x_title(p):
    if not p or p.get("type") != "title": return None
    v = _plain(p.get("title") or [])
    return v or None

def _x_text(p):
    if not p: return None
    t = p.get("type")
    if t == "rich_text": return _plain(p.get("rich_text") or []) or None
    if t == "title": return _x_title(p)
    if t == "url": return p.get("url") or None
    if t == "email": return p.get("email") or None
    if t == "phone_number": return p.get("phone_number") or None
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "string": return (f.get("string") or "").strip() or None
    return None

def _x_select(p):
    if not p: return None
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
        if f.get("type") == "string": return (f.get("string") or "").strip() or None
    return None

def _x_number(p):
    if not p: return None
    t = p.get("type")
    if t == "number": return p.get("number")
    if t == "formula":
        f = p.get("formula") or {}
        if f.get("type") == "number": return f.get("number")
    if t == "rollup":
        r = p.get("rollup") or {}
        if r.get("type") == "number": return r.get("number")
        # Certains rollups renvoient une array de nombres — on somme
        if r.get("type") == "array":
            total = 0
            has_val = False
            for item in (r.get("array") or []):
                if item.get("type") == "number" and item.get("number") is not None:
                    total += item["number"]; has_val = True
            return total if has_val else None
    return None

def _x_date(p):
    if not p: return None
    t = p.get("type")
    if t == "date":
        d = p.get("date") or {}
        return d.get("start") or None
    return None

def _x_relation_ids(p):
    if not p or p.get("type") != "relation": return []
    return [r["id"] for r in (p.get("relation") or []) if r.get("id")]

def _x_place(p):
    if not p: return None
    t = p.get("type")
    if t == "place":
        pl = p.get("place") or {}
        return pl.get("address") or pl.get("name") or None
    if t == "rich_text":
        return _plain(p.get("rich_text") or []) or None
    return None

def _x_people(p):
    """Retourne une liste de noms (fallback: id tronqué). Liste vide si champ absent.
    Nécessite que l'intégration Notion ait la permission 'user information'
    pour que .name soit disponible, sinon on se rabat sur l'id."""
    if not p or p.get("type") != "people":
        return []
    out = []
    for u in (p.get("people") or []):
        name = u.get("name")
        if name:
            out.append(name)
        else:
            # fallback : affiche les 6 premiers caractères de l'id
            uid = u.get("id", "")
            out.append("User " + uid[:6] if uid else "—")
    return out

def _x_year(p):
    if not p: return None
    t = p.get("type")
    if t == "number":
        n = p.get("number")
        return int(n) if n is not None else None
    if t == "select":
        s = p.get("select") or {}
        name = s.get("name") or ""
        m = re.search(r"(\d{4})", name)
        return int(m.group(1)) if m else None
    if t == "rich_text":
        txt = _plain(p.get("rich_text") or [])
        m = re.search(r"(\d{4})", txt)
        return int(m.group(1)) if m else None
    return None

def _x_trl(p):
    if not p: return 0
    t = p.get("type")
    if t == "number":
        n = p.get("number")
        return int(n) if n else 0
    if t == "select":
        s = p.get("select") or {}
        name = s.get("name") or ""
        m = re.search(r"(\d+)", name)
        return int(m.group(1)) if m else 0
    return 0


def _x_checkbox(p):
    if not p or p.get("type") != "checkbox":
        return False
    return bool(p.get("checkbox", False))


# ======== HELPERS ========

def _try(page, names, extractor):
    for name in names:
        p = _prop(page, name)
        if p is None: continue
        v = extractor(p)
        if v not in (None, "", []): return v
    return None

def _try_relation(page, names):
    for name in names:
        p = _prop(page, name)
        if p is None: continue
        v = _x_relation_ids(p)
        if v: return v
    return []

def _norm(i):
    return i.replace("-", "") if i else i

def _resolve_name(page, name_by_id, names_candidates):
    ids = _try_relation(page, names_candidates)
    for i in ids:
        k = _norm(i)
        if k in name_by_id: return name_by_id[k]
    return _try(page, names_candidates, _x_text) or ""

def _resolve_names_for_ids(ids, name_by_id):
    return [name_by_id[_norm(i)] for i in ids if _norm(i) in name_by_id]


# ======== TRANSFORMERS ========

def transform_societe(page):
    return {
        "id":     _norm(page["id"]),
        "nom":    _try(page, ["Nom"], _x_title) or "",
        "statut": _try(page, ["Statut"], _x_select) or "Client",
        "lieu":   _try(page, ["Lieu"], _x_place) or "",
    }


def transform_projet(page, societe_by_id):
    score_r = _try(page, ["Score R"], _x_number) or 0
    score_i = _try(page, ["Score I"], _x_number) or 0
    score_d = _try(page, ["Score D"], _x_number) or 0
    axe = _try(page, ["Axe R&D&I", "Axe de R&DI"], _x_text) \
          or _try(page, ["Axe R&D&I", "Axe de R&DI"], _x_select)
    if not axe:
        parts = []
        if score_r: parts.append("R")
        if score_d: parts.append("D")
        if score_i: parts.append("I")
        axe = "&".join(parts) or "—"

    # Rollups projets : les 3 dispositifs sont désormais remontés séparément
    tot_cir_cii  = _try(page, ["Tot CIR/CII obtenu"], _x_number) or 0
    tot_cico     = _try(page, ["Tot CICO obtenu"], _x_number) or 0
    tot_subv     = _try(page, ["Tot Sub obtenu"], _x_number) or 0
    tot_engagees = _try(page, ["Tot Dépenses Engagées"], _x_number) or 0
    tot_valo     = _try(page, ["Tot dépenses valorisées"], _x_number) or 0

    return {
        "id":           _norm(page["id"]),
        "societe":      _resolve_name(page, societe_by_id, ["Société 2026"]),
        "nom":          _try(page, ["Nom"], _x_title) or "",
        "objectif":     _try(page, ["Objectif"], _x_text) or "",
        "verrous":      _try(page, ["Verrous"], _x_text) or "",
        "trl":          _x_trl(_prop(page, "TRL")),
        "trl_cible":    _x_trl(_prop(page, "TRL Cible")),
        "gouvernance":  _try(page, ["Gouvernance"], _x_select) or "Interne",
        "strategie_pi": _try(page, ["Stratégie PI"], _x_select) or "N.A",
        "demarrage":    _x_year(_prop(page, "Démarrage")),
        "cloture":      _x_year(_prop(page, "Cloture")),
        "score_r":      int(score_r),
        "score_i":      int(score_i),
        "score_d":      int(score_d),
        "axe_rdi":      axe,
        "tot_cir_cii_rollup":         tot_cir_cii,
        "tot_cico_rollup":            tot_cico,
        "tot_subv_rollup":            tot_subv,
        "tot_ci_rollup":              tot_cir_cii + tot_cico,  # agrégat CI
        "tot_financement_public":     tot_cir_cii + tot_cico + tot_subv,
        "tot_engagees_rollup":        tot_engagees,
        "tot_valo_rollup":            tot_valo,
    }


def transform_dossier(page, societe_by_id):
    """Chaque dossier porte des rollups (totaux sur ses jalons) + responsable PDJ.
    Les 2 dispositifs CIR/CII et CICO sont désormais remontés séparément."""
    mt_cir_cii = _try(page, ["Montant CIR/CII"], _x_number) or 0
    mt_cico    = _try(page, ["Montant CICO"], _x_number) or 0
    return {
        "id":      _norm(page["id"]),
        "nom":     _try(page, ["Nom"], _x_title) or "",
        "societe": _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type":    _try(page, ["Type"], _x_select) or "CIR",
        "annee":   _x_year(_prop(page, "Année")),
        # Responsable PDJ (people)
        "assigne": _x_people(_prop(page, "Personne")),
        # Rollups sur les jalons du dossier
        "depenses_engagees_rollup":     _try(page, ["Dépenses engagées"], _x_number) or 0,
        "depenses_valorisables_rollup": _try(page, ["Dépenses valorisables"], _x_number) or 0,
        "montant_cir_cii_rollup":       mt_cir_cii,
        "montant_cico_rollup":          mt_cico,
        "montant_ci_rollup":            mt_cir_cii + mt_cico,  # agrégat pour compat
        "subvention_percue_rollup":     _try(page, ["Subvention perçue"], _x_number) or 0,
    }


def transform_jalon(page, societe_by_id, projet_by_id):
    """Un jalon peut être relié à PLUSIEURS dossiers → on expose dossiers_ids.
    Un jalon peut cumuler CIR/CII + CICO + subvention sur les mêmes dépenses."""
    proj_ids = _try_relation(page, ["Projets 2026"])
    projet_nom = ""
    for i in proj_ids:
        k = _norm(i)
        if k in projet_by_id:
            projet_nom = projet_by_id[k]; break
    if not projet_nom:
        projet_nom = _try(page, ["Nom"], _x_title) or ""

    av = _try(page, ["Avancement"], _x_number)
    if av is not None and av > 1.5:
        av = av / 100.0

    doss_ids = _try_relation(page, ["Dossiers 2026"])

    montant_cir_cii = _try(page, ["Montant CIR/CII"], _x_number) or 0
    montant_cico    = _try(page, ["Montant CICO"], _x_number) or 0
    subvention      = _try(page, ["Subvention perçue"], _x_number) or 0

    return {
        "id":                    _norm(page["id"]),
        "projet":                projet_nom,
        "projets_ids":           [_norm(i) for i in proj_ids],
        "societe":               _resolve_name(page, societe_by_id, ["Société 2026"]),
        "annee":                 _x_year(_prop(page, "Année")),
        "type_ci":               _try(page, ["type CI"], _x_select) or "",
        "depenses_engagees":     _try(page, ["Dépenses engagées"], _x_number) or 0,
        "depenses_valorisables": _try(page, ["Dépenses Valorisable", "Dépenses valorisables"], _x_number) or 0,
        # Dispositifs potentiellement cumulés sur un même jalon
        "montant_cir_cii":       montant_cir_cii,
        "montant_cico":          montant_cico,
        "subvention_percue":     subvention,
        # Total du financement public du jalon (utile partout)
        "montant_ci":            montant_cir_cii + montant_cico,
        "financement_public":    montant_cir_cii + montant_cico + subvention,
        "avancement":            av,
        "certifie":              _x_checkbox(_prop(page, "certifié")),
        "dossiers_ids":          [_norm(i) for i in doss_ids],
    }


def transform_facture(page, societe_by_id):
    doss_ids = _try_relation(page, ["Dossiers 2026"])
    return {
        "id":           _norm(page["id"]),
        "nom":          _try(page, ["Nom"], _x_title) or "",
        "societe":      _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type":         _try(page, ["Type"], _x_select) or "Autre",
        "etat":         _try(page, ["État"], _x_select) or "A facturer",
        "montant":      _try(page, ["Montant"], _x_number) or 0,
        "exercice":     _x_year(_prop(page, "Exercice")),
        "date":         _try(page, ["Date de facturation"], _x_date),
        "dossiers_ids": [_norm(i) for i in doss_ids],
    }


def transform_risque(page, societe_by_id, projet_by_id):
    proj_ids = _try_relation(page, ["Projets 2026"])
    return {
        "id":             _norm(page["id"]),
        "nom":            _try(page, ["Nom"], _x_title) or "",
        "societe":        _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type_alerte":    _try(page, ["Type d'alerte"], _x_select) or "",
        "severite":       _try(page, ["Sévérité"], _x_select) or "Info",
        "statut":         _try(page, ["Statut"], _x_select) or "À traiter",
        "montant_expose": _try(page, ["Montant exposé (€)"], _x_number) or 0,
        "date_evenement": _try(page, ["Date événement"], _x_date),
        "date_limite":    _try(page, ["Date limite action"], _x_date),
        "actions":        _try(page, ["Actions à mener"], _x_text) or "",
        "projets_lies":   _resolve_names_for_ids(proj_ids, projet_by_id),
        "projets_ids":    [_norm(i) for i in proj_ids],
    }


def transform_contact(page, societe_by_id):
    return {
        "id":          _norm(page["id"]),
        "prenom":      _try(page, ["Prénom"], _x_title) or "",
        "nom_famille": _try(page, ["N. Famille"], _x_text) or "",
        "email":       _try(page, ["Email Address"], _x_text) or "",
        "phone":       _try(page, ["Phone Number"], _x_text) or "",
        "fonction":    _try(page, ["Fonction"], _x_select) or "",
        "societe":     _resolve_name(page, societe_by_id, ["Société 2026"]),
    }


def transform_livrable(page, societe_by_id, projet_by_id):
    proj_ids = _try_relation(page, ["Projets 2026"])
    doss_ids = _try_relation(page, ["Dossiers 2026"])
    return {
        "id":           _norm(page["id"]),
        "nom":          _try(page, ["Nom"], _x_title) or "",
        "societe":      _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type":         _try(page, ["Type"], _x_select) or "",
        "etat":         _try(page, ["Etat"], _x_select) or "En cours",
        "priorite":     _try(page, ["Priorité"], _x_select) or "Moyenne",
        "deadline":     _try(page, ["Deadline"], _x_date),
        "assigne":      _x_people(_prop(page, "Assigned To")),
        "projets_lies": _resolve_names_for_ids(proj_ids, projet_by_id),
        "projets_ids":  [_norm(i) for i in proj_ids],
        "dossiers_ids": [_norm(i) for i in doss_ids],
    }


def transform_document(page, societe_by_id):
    proj_ids = _try_relation(page, ["Projets 2026"])
    doss_ids = _try_relation(page, ["Dossiers 2026"])
    liv_ids  = _try_relation(page, ["Livrables 2026"])
    return {
        "id":            _norm(page["id"]),
        "nom":           _try(page, ["Nom"], _x_title) or "",
        "societe":       _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type":          _try(page, ["Type"], _x_select) or "Autre",
        "url":           _try(page, ["URL"], _x_text) or "",
        "projets_ids":   [_norm(i) for i in proj_ids],
        "dossiers_ids":  [_norm(i) for i in doss_ids],
        "livrables_ids": [_norm(i) for i in liv_ids],
    }


def transform_reunion(page, societe_by_id, projet_by_id, contact_by_id=None):
    proj_ids = _try_relation(page, ["Projets 2026"])
    liv_ids  = _try_relation(page, ["Livrables 2026"])
    cont_ids = _try_relation(page, ["Contacts 2026"])
    doss_ids = _try_relation(page, ["Dossiers 2026"])
    contacts_lies = _resolve_names_for_ids(cont_ids, contact_by_id) if contact_by_id else []
    return {
        "id":             _norm(page["id"]),
        "nom":            _try(page, ["Nom"], _x_title) or "",
        "societe":        _resolve_name(page, societe_by_id, ["Société 2026"]),
        "type":           _try(page, ["Type"], _x_select) or "Autre",
        "date":           _try(page, ["Date"], _x_date),
        "priorite":       _try(page, ["Priorité"], _x_select) or "",
        "statut":         _try(page, ["Statut"], _x_select) or "",
        "participants":   _x_people(_prop(page, "Participants")),
        "contacts_lies":  contacts_lies,
        "projets_lies":   _resolve_names_for_ids(proj_ids, projet_by_id),
        "projets_ids":    [_norm(i) for i in proj_ids],
        "livrables_ids":  [_norm(i) for i in liv_ids],
        "contacts_ids":   [_norm(i) for i in cont_ids],
        "dossiers_ids":   [_norm(i) for i in doss_ids],
    }


# ======== ORCHESTRATEUR ========

def _fetch_all_parallel():
    raw = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(query_database, dbid): key for key, dbid in DATABASES.items()}
        for fut in concurrent.futures.as_completed(futures):
            key = futures[fut]
            try: raw[key] = fut.result()
            except Exception as e:
                raw[key] = []
                print(f"[sync] erreur {key}: {e}", flush=True)
    return raw


def sync_all(curated=None):
    t0 = time.time()
    raw = _fetch_all_parallel()

    societes = [transform_societe(p) for p in raw.get("societes", [])]
    societe_by_id = {s["id"]: s["nom"] for s in societes}

    projets = [transform_projet(p, societe_by_id) for p in raw.get("projets", [])]
    projet_by_id = {p["id"]: p["nom"] for p in projets}

    dossiers = [transform_dossier(p, societe_by_id) for p in raw.get("dossiers", [])]
    jalons = [transform_jalon(p, societe_by_id, projet_by_id) for p in raw.get("jalons", [])]
    factures = [transform_facture(p, societe_by_id) for p in raw.get("factures", [])]
    risques = [transform_risque(p, societe_by_id, projet_by_id) for p in raw.get("risques", [])]
    contacts = [transform_contact(p, societe_by_id) for p in raw.get("contacts", [])]
    # Pour résoudre les noms des contacts dans les réunions
    contact_by_id = {c["id"]: (c["prenom"] + " " + c["nom_famille"]).strip() or c.get("email", "—") for c in contacts}
    livrables = [transform_livrable(p, societe_by_id, projet_by_id) for p in raw.get("livrables", [])]
    documents = [transform_document(p, societe_by_id) for p in raw.get("documents", [])]
    reunions = [transform_reunion(p, societe_by_id, projet_by_id, contact_by_id) for p in raw.get("reunions", [])]

    result = {
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "Notion — Page 2026 (pointdujourconseil)",
            "page_url": PAGE_URL,
            "databases": DATABASES,
            "counts": {
                "societes": len(societes), "projets": len(projets), "dossiers": len(dossiers),
                "jalons": len(jalons), "factures": len(factures), "risques": len(risques),
                "contacts": len(contacts), "livrables": len(livrables),
                "documents": len(documents), "reunions": len(reunions),
            },
            "sync_duration_s": round(time.time() - t0, 2),
        },
        "societes": societes, "projets": projets, "dossiers": dossiers,
        "jalons": jalons, "factures": factures, "risques": risques,
        "contacts": contacts, "livrables": livrables,
        "documents": documents, "reunions": reunions,
    }

    if curated:
        result["interpretations"] = curated.get("interpretations", {})
        result["eligibilites"]    = curated.get("eligibilites", {})

    return result


def inspect_schemas():
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(retrieve_database, dbid): key for key, dbid in DATABASES.items()}
        for fut in concurrent.futures.as_completed(futs):
            key = futs[fut]
            try:
                meta = fut.result()
                props = meta.get("properties", {}) or {}
                out[key] = [{"name": n, "type": i.get("type")} for n, i in sorted(props.items())]
            except Exception as e:
                out[key] = {"error": str(e)}
    return out

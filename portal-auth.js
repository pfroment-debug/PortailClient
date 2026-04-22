/* portal-auth.js — Overlay d'authentification pour le portail PDJ
 * -----------------------------------------------------------
 * À inclure en tête de <body> dans portail.html :
 *     <script src="/portal-auth.js"></script>
 *
 * Ce script :
 *   1. Intercepte window.fetch pour ajouter le header X-Portal-Password
 *      sur tous les appels /api/* (sauf /api/status qui est public).
 *   2. Au chargement, interroge /api/status pour savoir si un mot de passe
 *      est requis. Si oui et que le client n'en a pas encore stocké un
 *      valide, affiche un overlay de login.
 *   3. Stocke le mot de passe dans sessionStorage (pas localStorage) :
 *      il faut se reconnecter à chaque fermeture d'onglet.
 */

(function () {
  "use strict";

  const STORAGE_KEY = "pdj_portal_password";
  const HEADER_NAME = "X-Portal-Password";

  // -- 1. Intercepte fetch pour injecter le mot de passe ------------------
  const originalFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    init = init || {};
    try {
      const url = typeof input === "string" ? input : (input.url || "");
      if (url.startsWith("/api/") && !url.startsWith("/api/status")) {
        const pwd = sessionStorage.getItem(STORAGE_KEY) || "";
        if (pwd) {
          init.headers = new Headers(init.headers || {});
          init.headers.set(HEADER_NAME, pwd);
        }
      }
    } catch (e) { /* silencieux */ }
    return originalFetch(input, init);
  };

  // -- 2. Overlay UI ------------------------------------------------------
  function showLogin(errorMsg) {
    if (document.getElementById("pdj-login-overlay")) return;

    const css = `
      #pdj-login-overlay {
        position: fixed; inset: 0; z-index: 99999;
        background: #090B33; display: flex; align-items: center;
        justify-content: center; padding: 24px;
        font-family: "Montserrat", -apple-system, "Segoe UI", Roboto, sans-serif;
      }
      #pdj-login-card {
        background: #FFFFFF; border-radius: 8px; padding: 40px 36px;
        max-width: 420px; width: 100%;
        box-shadow: 0 20px 60px rgba(0,0,0,0.4);
        position: relative; overflow: hidden;
      }
      #pdj-login-card::before {
        content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
        background: linear-gradient(90deg, #090B33 0%, #1800FF 35%, #00A668 70%, #D6175E 100%);
      }
      #pdj-login-brand {
        display: flex; align-items: center; gap: 14px; margin-bottom: 28px;
      }
      #pdj-login-logo {
        width: 44px; height: 44px; border-radius: 8px; background: #090B33;
        color: #fff; font-weight: 800; font-size: 13px; display: flex;
        align-items: center; justify-content: center; letter-spacing: -0.3px;
      }
      #pdj-login-title {
        font-size: 18px; font-weight: 800; color: #090B33;
        text-transform: uppercase; letter-spacing: 0; line-height: 1.15;
      }
      #pdj-login-sub {
        font-size: 11px; color: #6B6E8A; margin-top: 4px; font-weight: 500;
        letter-spacing: 0.5px; text-transform: uppercase;
      }
      #pdj-login-label {
        display: block; font-size: 10px; color: #6B6E8A; font-weight: 700;
        letter-spacing: 1.5px; text-transform: uppercase; margin-bottom: 8px;
      }
      #pdj-login-input {
        width: 100%; padding: 12px 14px; border: 1px solid #D9DBE5;
        border-radius: 4px; font-size: 14px; font-family: inherit;
        color: #090B33; background: #F7F8FA; transition: border-color 0.15s;
        box-sizing: border-box;
      }
      #pdj-login-input:focus {
        outline: none; border-color: #1800FF; background: #FFFFFF;
      }
      #pdj-login-btn {
        width: 100%; margin-top: 16px; padding: 13px 16px;
        background: #090B33; color: #fff; border: none; border-radius: 4px;
        font-size: 12px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 1.5px; cursor: pointer; font-family: inherit;
        transition: background 0.15s;
      }
      #pdj-login-btn:hover { background: #1800FF; }
      #pdj-login-btn:disabled { opacity: 0.6; cursor: wait; }
      #pdj-login-err {
        margin-top: 12px; padding: 10px 12px; background: #FCDEE7;
        color: #8B0E3D; border-left: 3px solid #D6175E; border-radius: 2px;
        font-size: 12px; font-weight: 500; display: none;
      }
      #pdj-login-err.show { display: block; }
      #pdj-login-note {
        margin-top: 24px; padding-top: 18px; border-top: 1px solid #E7E8EF;
        font-size: 11px; color: #6B6E8A; line-height: 1.5;
      }
    `;

    const style = document.createElement("style");
    style.textContent = css;
    document.head.appendChild(style);

    const overlay = document.createElement("div");
    overlay.id = "pdj-login-overlay";
    overlay.innerHTML = `
      <div id="pdj-login-card">
        <div id="pdj-login-brand">
          <div id="pdj-login-logo">PDJ</div>
          <div>
            <div id="pdj-login-title">Point du Jour Conseil</div>
            <div id="pdj-login-sub">Portail client — accès restreint</div>
          </div>
        </div>
        <label id="pdj-login-label" for="pdj-login-input">Mot de passe d'accès</label>
        <input id="pdj-login-input" type="password" autocomplete="current-password"
               placeholder="••••••••" autofocus />
        <button id="pdj-login-btn" type="button">Accéder au portail</button>
        <div id="pdj-login-err"></div>
        <div id="pdj-login-note">
          Cet espace regroupe des données fiscales et R&amp;D de clients PDJ.
          Le mot de passe vous a été transmis par Point du Jour Conseil.
        </div>
      </div>
    `;
    document.body.appendChild(overlay);

    const input = document.getElementById("pdj-login-input");
    const btn = document.getElementById("pdj-login-btn");
    const err = document.getElementById("pdj-login-err");

    if (errorMsg) {
      err.textContent = errorMsg;
      err.classList.add("show");
    }

    async function tryLogin() {
      const pwd = (input.value || "").trim();
      if (!pwd) {
        err.textContent = "Veuillez saisir le mot de passe.";
        err.classList.add("show");
        return;
      }
      btn.disabled = true;
      btn.textContent = "Vérification…";
      err.classList.remove("show");

      try {
        const r = await originalFetch("/api/status", {
          method: "GET",
          headers: { [HEADER_N

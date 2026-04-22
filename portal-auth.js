/* portal-auth.js — Overlay d'authentification PDJ */

(function () {
  "use strict";

  var STORAGE_KEY = "pdj_portal_password";
  var HEADER_NAME = "X-Portal-Password";

  var originalFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    init = init || {};
    try {
      var url = typeof input === "string" ? input : (input.url || "");
      if (url.indexOf("/api/") === 0 && url.indexOf("/api/status") !== 0) {
        var pwd = sessionStorage.getItem(STORAGE_KEY) || "";
        if (pwd) {
          init.headers = new Headers(init.headers || {});
          init.headers.set(HEADER_NAME, pwd);
        }
      }
    } catch (e) {}
    return originalFetch(input, init).then(function (r) {
      try {
        var u = typeof input === "string" ? input : (input.url || "");
        if (u.indexOf("/api/") === 0 && r.status === 401) {
          sessionStorage.removeItem(STORAGE_KEY);
          showLogin("Session expirée ou mot de passe incorrect.");
        }
      } catch (e) {}
      return r;
    });
  };

  function showLogin(errorMsg) {
    if (document.getElementById("pdj-login-overlay")) {
      if (errorMsg) {
        var e = document.getElementById("pdj-login-err");
        if (e) { e.textContent = errorMsg; e.classList.add("show"); }
      }
      return;
    }

    var css = ''
      + '#pdj-login-overlay { position: fixed; inset: 0; z-index: 2147483647;'
      + '  background: #090B33; display: flex; align-items: center;'
      + '  justify-content: center; padding: 24px;'
      + '  font-family: "Montserrat", -apple-system, sans-serif; }'
      + '#pdj-login-card { background: #fff; border-radius: 8px;'
      + '  padding: 40px 36px; max-width: 420px; width: 100%;'
      + '  box-shadow: 0 20px 60px rgba(0,0,0,0.4);'
      + '  position: relative; overflow: hidden; }'
      + '#pdj-login-card::before { content: ""; position: absolute;'
      + '  top: 0; left: 0; right: 0; height: 3px;'
      + '  background: linear-gradient(90deg,#090B33 0%,#1800FF 35%,#00A668 70%,#D6175E 100%); }'
      + '#pdj-login-brand { display: flex; align-items: center; gap: 14px; margin-bottom: 28px; }'
      + '#pdj-login-logo { width: 44px; height: 44px; border-radius: 8px;'
      + '  background: #090B33; color: #fff; font-weight: 800; font-size: 13px;'
      + '  display: flex; align-items: center; justify-content: center; }'
      + '#pdj-login-title { font-size: 18px; font-weight: 800; color: #090B33;'
      + '  text-transform: uppercase; line-height: 1.15; }'
      + '#pdj-login-sub { font-size: 11px; color: #6B6E8A; margin-top: 4px;'
      + '  font-weight: 500; letter-spacing: 0.5px; text-transform: uppercase; }'
      + '#pdj-login-label { display: block; font-size: 10px; color: #6B6E8A;'
      + '  font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; margin-bottom: 8px; }'
      + '#pdj-login-input { width: 100%; padding: 12px 14px;'
      + '  border: 1px solid #D9DBE5; border-radius: 4px; font-size: 14px;'
      + '  font-family: inherit; color: #090B33; background: #F7F8FA;'
      + '  box-sizing: border-box; }'
      + '#pdj-login-input:focus { outline: none; border-color: #1800FF; background: #fff; }'
      + '#pdj-login-btn { width: 100%; margin-top: 16px; padding: 13px 16px;'
      + '  background: #090B33; color: #fff; border: none; border-radius: 4px;'
      + '  font-size: 12px; font-weight: 700; text-transform: uppercase;'
      + '  letter-spacing: 1.5px; cursor: pointer; font-family: inherit; }'
      + '#pdj-login-btn:hover { background: #1800FF; }'
      + '#pdj-login-btn:disabled { opacity: 0.6; cursor: wait; }'
      + '#pdj-login-err { margin-top: 12px; padding: 10px 12px; background: #FCDEE7;'
      + '  color: #8B0E3D; border-left: 3px solid #D6175E; border-radius: 2px;'
      + '  font-size: 12px; display: none; }'
      + '#pdj-login-err.show { display: block; }'
      + '#pdj-login-note { margin-top: 24px; padding-top: 18px;'
      + '  border-top: 1px solid #E7E8EF; font-size: 11px;'
      + '  color: #6B6E8A; line-height: 1.5; }';

    var style = document.createElement("style");
    style.textContent = css;
    document.head.appendChild(style);

    var overlay = document.createElement("div");
    overlay.id = "pdj-login-overlay";
    overlay.innerHTML = ''
      + '<div id="pdj-login-card">'
      + '  <div id="pdj-login-brand">'
      + '    <div id="pdj-login-logo">PDJ</div>'
      + '    <div>'
      + '      <div id="pdj-login-title">Point du Jour Conseil</div>'
      + '      <div id="pdj-login-sub">Portail client</div>'
      + '    </div>'
      + '  </div>'
      + '  <label id="pdj-login-label">Mot de passe d\'accès</label>'
      + '  <input id="pdj-login-input" type="password" placeholder="••••••••" autofocus />'
      + '  <button id="pdj-login-btn" type="button">Accéder au portail</button>'
      + '  <div id="pdj-login-err"></div>'
      + '  <div id="pdj-login-note">Le mot de passe vous a été transmis par Point du Jour Conseil.</div>'
      + '</div>';

    function attach() {
      document.body.appendChild(overlay);
      var input = document.getElementById("pdj-login-input");
      var btn = document.getElementById("pdj-login-btn");
      var err = document.getElementById("pdj-login-err");
      if (errorMsg) { err.textContent = errorMsg; err.classList.add("show"); }

      function tryLogin() {
        var pwd = (input.value || "").trim();
        if (!pwd) {
          err.textContent = "Veuillez saisir le mot de passe.";
          err.classList.add("show");
          return;
        }
        btn.disabled = true;
        btn.textContent = "Vérification…";
        err.classList.remove("show");
        var headers = {};
        headers[HEADER_NAME] = pwd;
        originalFetch("/api/data", { method: "GET", headers: headers, cache: "no-store" })
          .then(function (r) {
            if (r.status === 401) throw new Error("Mot de passe incorrect.");
            if (!r.ok) throw new Error("Erreur serveur : " + r.status);
            sessionStorage.setItem(STORAGE_KEY, pwd);
            location.reload();
          })
          .catch(function (e) {
            err.textContent = e.message || "Échec";
            err.classList.add("show");
            btn.disabled = false;
            btn.textContent = "Accéder au portail";
          });
      }
      btn.addEventListener("click", tryLogin);
      input.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") tryLogin();
      });
      input.focus();
    }

    if (document.body) attach();
    else document.addEventListener("DOMContentLoaded", attach);
  }

  window.pdjShowLogin = showLogin;

  // Affichage immédiat si pas de mot de passe stocké
  if (!sessionStorage.getItem(STORAGE_KEY)) {
    if (document.body) showLogin();
    else document.addEventListener("DOMContentLoaded", function () { showLogin(); });
  }
})();

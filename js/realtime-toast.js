window.ProctorToast = (() => {
    let host = null;
    const seen = new Map();

    function ensureHost() {
        if (host) return host;
        host = document.createElement("div");
        host.id = "global-toast-host";
        host.style.cssText = "position:fixed;top:16px;right:16px;z-index:99999;display:flex;flex-direction:column;gap:10px;max-width:360px";
        document.body.appendChild(host);
        return host;
    }

    function show({ id, title, message, level = "warning", ttl = 5000 }) {
        const now = Date.now();
        const dedupeKey = id || `${title}:${message}`;
        const prev = seen.get(dedupeKey) || 0;
        if (now - prev < 6000) return;
        seen.set(dedupeKey, now);

        const root = ensureHost();
        const item = document.createElement("div");
        const bg = level === "danger" ? "#7f1d1d" : level === "success" ? "#14532d" : "#1e3a8a";
        item.style.cssText = `background:${bg};color:#fff;padding:12px 14px;border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,.25);font-family:Inter,sans-serif`;
        item.innerHTML = `<div style="font-weight:700;font-size:13px">${escapeHtml(title || "Notification")}</div><div style="font-size:12px;margin-top:4px;line-height:1.4">${escapeHtml(message || "")}</div>`;
        root.appendChild(item);
        setTimeout(() => item.remove(), ttl);
    }

    function escapeHtml(value) {
        return String(value ?? "").replace(/[&<>"']/g, (s) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[s]));
    }

    return { show };
})();


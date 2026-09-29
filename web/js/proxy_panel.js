import { app } from "/scripts/app.js";

const POS_KEY = "comfyui_proxy_panel_pos";

function loadPos() {
    try {
        return JSON.parse(localStorage.getItem(POS_KEY)) || { x: 20, y: 80 };
    } catch (e) {
        return { x: 20, y: 80 };
    }
}

function savePos(pos) {
    try {
        localStorage.setItem(POS_KEY, JSON.stringify(pos));
    } catch (e) {
        /* ignore */
    }
}

app.registerExtension({
    name: "ComfyUIProxy.Panel",
    async setup() {
        const pos = loadPos();

        const root = document.createElement("div");
        root.id = "comfyui-proxy-root";
        Object.assign(root.style, {
            position: "fixed",
            left: pos.x + "px",
            top: pos.y + "px",
            zIndex: 9999,
            fontFamily: "sans-serif",
            userSelect: "none",
        });

        // --- Pill (toggle + drag handle) ---
        const pill = document.createElement("div");
        Object.assign(pill.style, {
            display: "flex",
            alignItems: "center",
            gap: "8px",
            background: "#2b2b2b",
            border: "1px solid #444",
            borderRadius: "20px",
            padding: "6px 12px",
            cursor: "grab",
            boxShadow: "0 2px 6px rgba(0,0,0,0.4)",
            color: "#ddd",
            fontSize: "12px",
        });

        const dot = document.createElement("span");
        Object.assign(dot.style, {
            width: "10px",
            height: "10px",
            borderRadius: "50%",
            background: "#888",
            display: "inline-block",
            transition: "background 0.2s",
            flexShrink: "0",
        });

        const label = document.createElement("span");
        label.textContent = "Cloud GPU";
        label.title = "Click to expand settings";
        label.style.cursor = "pointer";

        const toggle = document.createElement("input");
        toggle.type = "checkbox";
        toggle.title = "Enable/disable remote GPU forwarding";
        toggle.style.cursor = "pointer";

        pill.appendChild(dot);
        pill.appendChild(label);
        pill.appendChild(toggle);
        root.appendChild(pill);

        // --- Expandable settings panel ---
        const panel = document.createElement("div");
        Object.assign(panel.style, {
            display: "none",
            marginTop: "6px",
            background: "#2b2b2b",
            border: "1px solid #444",
            borderRadius: "8px",
            padding: "10px",
            width: "260px",
            boxShadow: "0 2px 6px rgba(0,0,0,0.4)",
            color: "#ddd",
            fontSize: "12px",
        });

        function field(labelText, type, placeholder) {
            const wrap = document.createElement("div");
            wrap.style.marginBottom = "8px";
            const l = document.createElement("label");
            l.textContent = labelText;
            Object.assign(l.style, { display: "block", marginBottom: "3px", opacity: "0.8" });
            const inp = document.createElement("input");
            inp.type = type;
            inp.placeholder = placeholder || "";
            Object.assign(inp.style, {
                width: "100%",
                boxSizing: "border-box",
                background: "#1b1b1b",
                border: "1px solid #444",
                borderRadius: "4px",
                color: "#eee",
                padding: "5px 7px",
            });
            wrap.appendChild(l);
            wrap.appendChild(inp);
            panel.appendChild(wrap);
            return inp;
        }

        const urlInput = field("Remote GPU URL", "text", "https://your-endpoint.example.com");
        const timeoutInput = field("Timeout (seconds)", "number", "120");
        const authInput = field("Auth Key (optional)", "password", "Bearer token");

        const statusLine = document.createElement("div");
        Object.assign(statusLine.style, { marginBottom: "8px", opacity: "0.75", fontSize: "11px", lineHeight: "1.4" });
        panel.appendChild(statusLine);

        const btnRow = document.createElement("div");
        Object.assign(btnRow.style, { display: "flex", gap: "6px" });

        function mkButton(text) {
            const b = document.createElement("button");
            b.textContent = text;
            Object.assign(b.style, {
                flex: "1",
                background: "#3a3a3a",
                border: "1px solid #555",
                borderRadius: "4px",
                color: "#eee",
                padding: "5px 0",
                cursor: "pointer",
            });
            b.onmouseenter = () => (b.style.background = "#4a4a4a");
            b.onmouseleave = () => (b.style.background = "#3a3a3a");
            return b;
        }

        const saveBtn = mkButton("Save");
        const refreshBtn = mkButton("Refresh Models");
        btnRow.appendChild(saveBtn);
        btnRow.appendChild(refreshBtn);
        panel.appendChild(btnRow);

        root.appendChild(panel);
        document.body.appendChild(root);

        let expanded = false;
        function setExpanded(v) {
            expanded = v;
            panel.style.display = expanded ? "block" : "none";
        }

        async function getConfig() {
            const r = await fetch("/comfyui_proxy/config");
            return r.json();
        }

        async function postConfig(patch) {
            const r = await fetch("/comfyui_proxy/config", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(patch),
            });
            return r.json();
        }

        function refreshUI(cfg) {
            toggle.checked = !!cfg.enabled;
            dot.style.background = cfg.enabled ? "#4caf50" : "#888";
            urlInput.value = cfg.remote_url || "";
            timeoutInput.value = cfg.timeout || 120;
            authInput.placeholder = cfg.auth_key_set ? "•••• saved (leave blank to keep)" : "Bearer token";
            statusLine.textContent = cfg.remote_url
                ? cfg.has_models_cache
                    ? "Remote model list cached."
                    : "Remote model list not yet cached."
                : "No remote URL configured yet.";
        }

        try {
            const cfg = await getConfig();
            refreshUI(cfg);
        } catch (e) {
            console.warn("[ComfyUI Proxy] Failed to load config", e);
        }

        toggle.addEventListener("change", async () => {
            const desired = toggle.checked;
            try {
                const cfg = await postConfig({ enabled: desired });
                refreshUI(cfg);
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to update toggle", e);
                toggle.checked = !desired;
            }
        });

        label.addEventListener("click", () => setExpanded(!expanded));

        saveBtn.addEventListener("click", async () => {
            const patch = {
                remote_url: urlInput.value.trim(),
                timeout: parseFloat(timeoutInput.value) || 120,
            };
            if (authInput.value.trim()) {
                patch.auth_key = authInput.value.trim();
            }
            saveBtn.textContent = "Saving...";
            try {
                const cfg = await postConfig(patch);
                refreshUI(cfg);
                authInput.value = "";
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to save config", e);
            }
            saveBtn.textContent = "Save";
        });

        refreshBtn.addEventListener("click", async () => {
            refreshBtn.textContent = "Refreshing...";
            try {
                await fetch("/comfyui_proxy/refresh_models", { method: "POST" });
                const cfg = await getConfig();
                refreshUI(cfg);
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to refresh models", e);
            }
            refreshBtn.textContent = "Refresh Models";
        });

        // --- Dragging the pill moves the whole panel ---
        let dragging = false;
        let startX, startY, origX, origY;

        pill.addEventListener("mousedown", (e) => {
            if (e.target === toggle || e.target === label) return;
            dragging = true;
            pill.style.cursor = "grabbing";
            startX = e.clientX;
            startY = e.clientY;
            const rect = root.getBoundingClientRect();
            origX = rect.left;
            origY = rect.top;
            e.preventDefault();
        });
        window.addEventListener("mousemove", (e) => {
            if (!dragging) return;
            const dx = e.clientX - startX;
            const dy = e.clientY - startY;
            const x = Math.max(0, origX + dx);
            const y = Math.max(0, origY + dy);
            root.style.left = x + "px";
            root.style.top = y + "px";
        });
        window.addEventListener("mouseup", () => {
            if (!dragging) return;
            dragging = false;
            pill.style.cursor = "grab";
            const rect = root.getBoundingClientRect();
            savePos({ x: rect.left, y: rect.top });
        });
    },
});

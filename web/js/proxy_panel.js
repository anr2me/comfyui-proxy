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

// --- Long waits for a cold-starting GPU --------------------------------------
// ComfyUI's frontend aborts any api.fetchApi() call that hasn't received response
// headers within 60 s ("TimeoutError: Fetch timeout", shown as a "Prompt execution
// failed" dialog). A serverless GPU can take longer than that to wake up, and
// submitting a prompt (or uploading an input file) waits for it. While the proxy
// is on, those two calls get the proxy's own Timeout setting (plus a margin)
// instead, so what the user ends up seeing is the proxy's real answer — or its
// real timeout error — rather than the browser giving up first.
const LONG_WAIT_MARGIN_MS = 30000;
const LONG_WAIT_ROUTES = [/^(?:\/api)?\/prompt$/, /^(?:\/api)?\/upload\//];
let longWaitMs = null; // null = proxy off: leave the frontend's own limit alone

function wrapFetchApiForLongWaits(api, getWaitMs) {
    const original = api.fetchApi.bind(api);
    api.fetchApi = (route, options) => {
        const waitMs = getWaitMs();
        if (waitMs && typeof route === "string" && !(options && "timeoutMs" in options)) {
            const method = String((options && options.method) || "GET").toUpperCase();
            if (method === "POST" && LONG_WAIT_ROUTES.some((re) => re.test(route.split("?")[0]))) {
                options = { ...options, timeoutMs: waitMs };
            }
        }
        return original(route, options);
    };
}
// --- end of long waits --------------------------------------------------------

app.registerExtension({
    name: "ComfyUIProxy.Panel",
    async setup() {
        const pos = loadPos();

        // Let the frontend wait as long as the proxy does for the calls that wake the GPU.
        try {
            const { api } = await import("/scripts/api.js");
            wrapFetchApiForLongWaits(api, () => longWaitMs);
        } catch (e) {
            console.warn("[ComfyUI Proxy] Could not extend the frontend's request timeout", e);
        }

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
            padding: "8px 12px",
            minHeight: "28px",
            cursor: "grab",
            boxShadow: "0 2px 6px rgba(0,0,0,0.4)",
            color: "#ddd",
            fontSize: "12px",
        });

        pill.style.touchAction = "none"; // let us handle drag gestures instead of the page scrolling

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
        label.textContent = "Remote GPU";
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

        function checkboxField(labelText, title) {
            const wrap = document.createElement("div");
            Object.assign(wrap.style, { display: "flex", alignItems: "center", gap: "6px", marginBottom: "8px" });
            const inp = document.createElement("input");
            inp.type = "checkbox";
            const l = document.createElement("label");
            l.textContent = labelText;
            Object.assign(l.style, { opacity: "0.9", cursor: "pointer" });
            if (title) {
                wrap.title = title;
            }
            l.addEventListener("click", () => {
                inp.checked = !inp.checked;
                inp.dispatchEvent(new Event("change"));
            });
            wrap.appendChild(inp);
            wrap.appendChild(l);
            panel.appendChild(wrap);
            return inp;
        }

        const urlInput = field("Remote GPU URL", "text", "https://your-endpoint.example.com");
        const authInput = field("Remote GPU Auth Key (optional)", "password", "Bearer token");
        const cpuUrlInput = field("Remote CPU URL (optional)", "text", "https://your-cpu-endpoint.example.com");
        const cpuAuthInput = field("Remote CPU Auth Key (optional)", "password", "Bearer token");
        const timeoutInput = field("Timeout (seconds)", "number", "300");
        timeoutInput.title =
            "How long the proxy waits for the remote GPU to answer, including a cold start. While the proxy is " +
            "on, the Run button (and file uploads) wait this long plus 30 seconds too, instead of ComfyUI's own " +
            "60-second limit.";
        const circuitCooldownInput = field(
            "Unresponsive-GPU polling cooldown (seconds)", "number", "30"
        );
        circuitCooldownInput.title =
            "After /queue or /api/jobs fails to respond, pauses automatic polling of it for this long " +
            "before trying again, instead of repeatedly re-attempting and keeping it looking \"active\" " +
            "to your serverless provider's own idle-timeout. Raise this if your provider's idle-timeout " +
            "is longer than the default.";
        const delayInput = field("Post-completion delay (seconds)", "number", "5");
        const jobsHistoryInput = field("Saved job history size (jobs)", "number", "500");
        jobsHistoryInput.title =
            "How many finished jobs to remember on this computer (in jobs_history.db), so the Media " +
            "Assets panel still lists them after ComfyUI or the remote restarts. Oldest are dropped first.";

        const autoDlInput = checkboxField(
            "Auto-download viewed input/output",
            "Saves files the UI views from the remote (type=output / type=input, as used by the Media " +
            "Assets panel) into your local ComfyUI output/input folder, and serves them from there " +
            "afterwards, so redrawing thumbnails (e.g. after switching browser tabs) doesn't wake the " +
            "remote again. Partial (Range) downloads are tracked in <filename>.tmp / <filename>.map " +
            "files until the whole file is present."
        );

        const keepaliveInput = checkboxField(
            "Keep GPU warm for video/image viewing",
            "Keeps the existing progress connection to the GPU open while /view or /viewvideo requests " +
            "keep arriving, so long video playback survives past the normal post-job grace window. Has a " +
            "real cost — off by default, and only useful if you don't have a Remote CPU URL configured above. " +
            "Usually unnecessary when \"Auto-download viewed input/output\" is on."
        );
        const keepaliveIdleInput = field("  View-activity idle timeout (seconds)", "number", "20");
        // The timeout only means something while "Keep GPU warm" is on, so keep it out of sight otherwise.
        const keepaliveIdleWrap = keepaliveIdleInput.parentElement;
        function syncKeepaliveIdleVisibility() {
            keepaliveIdleWrap.style.display = keepaliveInput.checked ? "" : "none";
        }
        keepaliveInput.addEventListener("change", syncKeepaliveIdleVisibility);
        syncKeepaliveIdleVisibility();

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

        const resetBtn = mkButton("Clear Stuck State");
        resetBtn.title = "Forgets any tracked job/connection the proxy thinks is still active, without restarting ComfyUI";
        Object.assign(resetBtn.style, { width: "100%", marginTop: "6px" });
        panel.appendChild(resetBtn);

        const resetConfigBtn = mkButton("Reset to Defaults");
        resetConfigBtn.title = "Resets URL, timeout, delay, history size, and auth key back to their defaults";
        Object.assign(resetConfigBtn.style, { width: "100%", marginTop: "6px", background: "#4a2a2a" });
        resetConfigBtn.onmouseenter = () => (resetConfigBtn.style.background = "#5a3333");
        resetConfigBtn.onmouseleave = () => (resetConfigBtn.style.background = "#4a2a2a");
        panel.appendChild(resetConfigBtn);

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

        async function refreshFrontendComboLists() {
            // The graph editor caches node defs (incl. dropdown options) from
            // /object_info at page load; newly patched server-side data won't
            // show up in existing OR new nodes until the frontend re-pulls
            // those defs. ComfyUI exposes app.refreshComboInNodes() for
            // exactly this (used by manager-style extensions after installing
            // models) — use it when available, otherwise fall back to a full
            // reload so the dropdown is guaranteed correct.
            try {
                if (typeof app.refreshComboInNodes === "function") {
                    await app.refreshComboInNodes();
                    return true;
                }
            } catch (e) {
                console.warn("[ComfyUI Proxy] refreshComboInNodes failed", e);
            }
            return false;
        }

        function refreshUI(cfg) {
            toggle.checked = !!cfg.enabled;
            dot.style.background = cfg.enabled ? "#4caf50" : "#888";
            longWaitMs = cfg.enabled ? (Number(cfg.timeout) || 300) * 1000 + LONG_WAIT_MARGIN_MS : null;
            urlInput.value = cfg.remote_url || "";
            cpuUrlInput.value = cfg.remote_cpu_url || "";
            timeoutInput.value = cfg.timeout || 300;
            delayInput.value = cfg.post_completion_delay ?? 5;
            jobsHistoryInput.value = cfg.jobs_history_max_entries ?? 500;
            circuitCooldownInput.value = cfg.circuit_breaker_cooldown ?? 30;
            autoDlInput.checked = !!cfg.auto_download_viewed;
            keepaliveInput.checked = !!cfg.gpu_keepalive_enabled;
            keepaliveIdleInput.value = cfg.gpu_keepalive_idle_timeout ?? 20;
            syncKeepaliveIdleVisibility();
            authInput.placeholder = cfg.auth_key_set ? "•••• saved (leave blank to keep)" : "Bearer token";
            cpuAuthInput.placeholder = cfg.remote_cpu_auth_key_set ? "•••• saved (leave blank to keep)" : "Bearer token";
            renderModelsStatus(cfg);
        }

        // Only the status line, so polling never overwrites what is being typed in the fields.
        function renderModelsStatus(cfg) {
            statusLine.textContent = cfg.remote_url
                ? cfg.models_refreshing
                    ? "Updating model list..."
                    : cfg.has_models_cache
                        ? `Remote model list cached (${cfg.models_cache_count} field${cfg.models_cache_count === 1 ? "" : "s"}).`
                        : "Remote model list not yet cached."
                : "No remote URL configured yet.";
        }

        // Saving or enabling starts the model list fetch in the background, so the
        // status stays on "Updating model list..." until the proxy says it is done
        // (a cold start can take a while), then shows the cached count — or goes
        // back to "not yet cached" if the list is still empty — and refreshes the
        // dropdowns like the Refresh Models button does.
        let modelsPollTimer = null;
        function watchModelsRefresh() {
            if (modelsPollTimer) return; // already watching
            const tick = async () => {
                let cfg;
                try {
                    cfg = await getConfig();
                } catch (e) {
                    modelsPollTimer = null;
                    return;
                }
                renderModelsStatus(cfg);
                if (cfg.models_refreshing) {
                    modelsPollTimer = setTimeout(tick, 1500);
                    return;
                }
                modelsPollTimer = null;
                // Also when nothing is cached (e.g. a new URL's first fetch failed), so the
                // dropdowns drop the previous remote's models instead of keeping them.
                const ok = await refreshFrontendComboLists();
                if (!ok) {
                    statusLine.textContent += " Reload the page to see updated model dropdowns.";
                }
            };
            modelsPollTimer = setTimeout(tick, 1000);
        }

        try {
            const cfg = await getConfig();
            refreshUI(cfg);
            if (cfg.models_refreshing) watchModelsRefresh();
        } catch (e) {
            console.warn("[ComfyUI Proxy] Failed to load config", e);
        }

        toggle.addEventListener("change", async () => {
            const desired = toggle.checked;
            try {
                const cfg = await postConfig({ enabled: desired });
                refreshUI(cfg);
                if (cfg.models_refreshing) watchModelsRefresh();
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to update toggle", e);
                toggle.checked = !desired;
            }
        });

        label.addEventListener("click", () => setExpanded(!expanded));

        saveBtn.addEventListener("click", async () => {
            const patch = {
                remote_url: urlInput.value.trim(),
                remote_cpu_url: cpuUrlInput.value.trim(),
                timeout: parseFloat(timeoutInput.value) || 300,
                post_completion_delay: parseFloat(delayInput.value),
                jobs_history_max_entries: parseInt(jobsHistoryInput.value, 10),
                circuit_breaker_cooldown: parseFloat(circuitCooldownInput.value),
                auto_download_viewed: autoDlInput.checked,
                gpu_keepalive_enabled: keepaliveInput.checked,
                gpu_keepalive_idle_timeout: parseFloat(keepaliveIdleInput.value),
            };
            if (isNaN(patch.post_completion_delay)) patch.post_completion_delay = 5;
            if (isNaN(patch.jobs_history_max_entries) || patch.jobs_history_max_entries < 1) patch.jobs_history_max_entries = 500;
            if (isNaN(patch.circuit_breaker_cooldown) || patch.circuit_breaker_cooldown < 1) patch.circuit_breaker_cooldown = 30;
            if (isNaN(patch.gpu_keepalive_idle_timeout) || patch.gpu_keepalive_idle_timeout < 5) patch.gpu_keepalive_idle_timeout = 20;
            if (authInput.value.trim()) {
                patch.auth_key = authInput.value.trim();
            }
            if (cpuAuthInput.value.trim()) {
                patch.remote_cpu_auth_key = cpuAuthInput.value.trim();
            }
            saveBtn.textContent = "Saving...";
            try {
                const cfg = await postConfig(patch);
                refreshUI(cfg);
                authInput.value = "";
                cpuAuthInput.value = "";
                if (cfg.models_refreshing) {
                    watchModelsRefresh(); // dropdowns are refreshed once the new list is in
                } else if (cfg.has_models_cache) {
                    const ok = await refreshFrontendComboLists();
                    if (!ok) {
                        statusLine.textContent += " Reload the page to see updated model dropdowns.";
                    }
                }
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to save config", e);
            }
            saveBtn.textContent = "Save";
        });

        refreshBtn.addEventListener("click", async () => {
            refreshBtn.textContent = "Refreshing...";
            statusLine.textContent = "Updating model list...";
            try {
                await fetch("/comfyui_proxy/refresh_models", { method: "POST" });
                const cfg = await getConfig();
                refreshUI(cfg);
                const ok = await refreshFrontendComboLists();
                if (!ok) {
                    statusLine.textContent += " Reload the page to see updated model dropdowns.";
                }
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to refresh models", e);
            }
            refreshBtn.textContent = "Refresh Models";
        });

        resetBtn.addEventListener("click", async () => {
            resetBtn.textContent = "Clearing...";
            try {
                const r = await fetch("/comfyui_proxy/reset_state", { method: "POST" });
                const result = await r.json();
                statusLine.textContent =
                    `Cleared ${result.cleared_jobs} tracked job(s), ` +
                    `cancelled ${result.cancelled_relays} connection(s).`;
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to clear stuck state", e);
                statusLine.textContent = "Failed to clear stuck state — see console.";
            }
            resetBtn.textContent = "Clear Stuck State";
        });

        resetConfigBtn.addEventListener("click", async () => {
            if (!confirm("Reset both remote URLs, timeout, delay, history size, polling cooldown, keep-alive settings, and both auth keys back to their defaults? This also disables the proxy.")) {
                return;
            }
            resetConfigBtn.textContent = "Resetting...";
            try {
                const cfg = await (await fetch("/comfyui_proxy/reset_config", { method: "POST" })).json();
                refreshUI(cfg);
                authInput.value = "";
                cpuAuthInput.value = "";
                statusLine.textContent = "Config reset to defaults.";
            } catch (e) {
                console.error("[ComfyUI Proxy] Failed to reset config", e);
                statusLine.textContent = "Failed to reset config — see console.";
            }
            resetConfigBtn.textContent = "Reset to Defaults";
        });

        // --- Dragging the pill moves the whole panel ---
        // Pointer Events unify mouse, touch, and pen in one code path, which
        // is what makes this work on mobile browsers (plain mouse* events
        // never fire there).
        let dragging = false;
        let moved = false;
        let startX, startY, origX, origY;
        const DRAG_THRESHOLD = 4; // px, so a tap still reaches the toggle/label

        pill.addEventListener("pointerdown", (e) => {
            if (e.target === toggle) return;
            dragging = true;
            moved = false;
            pill.setPointerCapture(e.pointerId);
            pill.style.cursor = "grabbing";
            startX = e.clientX;
            startY = e.clientY;
            const rect = root.getBoundingClientRect();
            origX = rect.left;
            origY = rect.top;
        });
        pill.addEventListener("pointermove", (e) => {
            if (!dragging) return;
            const dx = e.clientX - startX;
            const dy = e.clientY - startY;
            if (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD) {
                moved = true;
            }
            if (!moved) return;
            const maxX = window.innerWidth - root.offsetWidth;
            const maxY = window.innerHeight - 20;
            const x = Math.min(Math.max(0, origX + dx), Math.max(0, maxX));
            const y = Math.min(Math.max(0, origY + dy), Math.max(0, maxY));
            root.style.left = x + "px";
            root.style.top = y + "px";
            e.preventDefault();
        });
        function endDrag(e) {
            if (!dragging) return;
            dragging = false;
            pill.style.cursor = "grab";
            try {
                pill.releasePointerCapture(e.pointerId);
            } catch (err) {
                /* ignore */
            }
            const rect = root.getBoundingClientRect();
            savePos({ x: rect.left, y: rect.top });
            // Swallow the click that follows a real drag so it doesn't
            // toggle the expanded panel by accident; a plain tap still works
            // since `moved` stays false for it.
            if (moved) {
                const suppressClick = (ev) => {
                    ev.stopPropagation();
                    ev.preventDefault();
                    label.removeEventListener("click", suppressClick, true);
                };
                label.addEventListener("click", suppressClick, true);
                setTimeout(() => label.removeEventListener("click", suppressClick, true), 0);
            }
        }
        pill.addEventListener("pointerup", endDrag);
        pill.addEventListener("pointercancel", endDrag);
    },
});

# ComfyUI Proxy

Lets you run ComfyUI locally while sending the actual image/video generation
to a cloud or serverless GPU elsewhere. Editing your workflow, browsing
nodes, and the rest of the UI all stay fully local and work even with the
proxy off or no internet connection — only the parts that need a GPU
(queuing a prompt, checking progress, fetching the result) get sent to the
remote machine.

A small draggable pill in the corner of the screen lets you turn this on or
off, and expands into a settings panel.

<i><b>Note:</b> This custom node was mainly tested on ComfyUI deployed at Modal.com using https://github.com/anr2me/modal-comfyui  
(ComfyGPU's URL for Remote GPU URL, and optionally ComfyCPU's URL for Remote CPU URL).</i>

## Install

Clone this repository into `ComfyUI/custom_nodes/` directory and restart ComfyUI. 
```bash
cd ComfyUI/custom_nodes
git clone https://github.com/anr2me/comfyui-proxy.git
```
No extra setup needed — it only uses libraries ComfyUI already ships with.

If you ever see a decoding/compression-related error in the console, you
can optionally install `Brotli` + `backports.zstd` from `requirements.txt`:
```bash
cd comfyui-proxy
pip install -r requirements.txt
```

<i><b>Note:</b> It's recommended to install the same custom nodes on your local ComfyUI with the one available on your server side ComfyUI, so you can use those custom nodes on your workflow locally. 
Meanwhile, models list will be pulled from server side ComfyUI, thus you can't use your locally downloaded models when the Remote GPU is enabled, since the models might not be available on server side.</i>

## Settings

Tap the pill, then the label, to open the panel.

- **Remote GPU URL** — your cloud/serverless ComfyUI endpoint. Required to
  use the proxy at all.
- **Remote GPU Auth Key** *(optional)* — a bearer token, if your endpoint
  needs one.
- **Remote CPU URL** *(optional)* — a second, separate endpoint that shares
  the same storage as your GPU one but doesn't need a GPU itself. If you
  have one, uploading input images/videos and viewing/downloading output results can use this
  cheaper endpoint instead of waking the GPU just to serve a file, and the
  model list is fetched from it too. This
  only helps if it's a genuinely different, separately-running endpoint —
  pointing it at the same URL as your GPU one does nothing useful.
- **Remote CPU Auth Key** *(optional)* — bearer token for the CPU endpoint,
  if it needs one.
- **Timeout** — how long to wait for the remote to respond before giving
  up, in seconds. Serverless GPUs can take a while to cold-start when
  capacity is scarce, so this defaults fairly high (300s). Raise it if you
  see timeout errors right after queuing a prompt. While the proxy is on,
  the **Run** button (and file uploads) wait this long too — ComfyUI itself
  normally gives up after 60 seconds, which a cold start can easily outlast.
- **Unresponsive-GPU polling cooldown** — if the remote stops responding
  entirely, pauses automatic background checks for this many seconds before
  trying again, so a broken connection doesn't keep your serverless
  instance looking "busy" forever. If it still doesn't answer the next
  time, the proxy stops checking until you queue a new prompt or use
  **Clear Stuck State**. If your provider takes longer than the default
  (70s) to actually shut down an idle instance, raise this.
- **Post-completion delay** — after a job finishes, keeps the live progress
  connection open a few extra seconds so the progress bar and the final log
  lines (like "Prompt executed in …") finish arriving before disconnecting.
- **Saved job history size** — how many finished jobs to remember on your
  computer (500 by default). They're kept in `jobs_history.db` inside this
  folder, separately for each Remote GPU URL, so the Media Assets panel
  still lists your older jobs after ComfyUI or the remote restarts, without
  waking the remote. Deleting or clearing jobs in the panel removes them
  from it too.
- **Auto-download viewed input/output** *(on by default)* — saves the
  images and videos you view in the Media Assets panel to your local
  `output` / `input` folder. Once a file is saved, it's shown from your own
  computer, so switching browser tabs or reopening the panel no longer
  wakes the remote just to redraw thumbnails. Big files finish downloading
  in the background and pick up where they left off if interrupted (you may
  briefly see `.tmp` / `.map` files next to them), and the connection to the
  GPU stays open until they're done. ComfyUI is also asked to add the new
  files to your local Assets list. If a local file with the same name
  already exists, it's used as-is.
- **Keep GPU warm for video/image viewing** *(off by default)* — only
  relevant if you don't have a Remote CPU URL, and usually unnecessary if
  **Auto-download viewed input/output** is on. Normally, once a job
  finishes, the connection to the GPU closes after a few seconds and the
  GPU can shut down — which can cut off a video you're still watching.
  Turning this on keeps that connection open for as long as you keep
  viewing images/videos, so playback doesn't get cut off. This has a real
  cost, since it keeps paying for the GPU while you're watching.
  - **View-activity idle timeout** *(appears once the option above is
    ticked)* — how long to keep the connection open after the last time
    you viewed something, before giving up.

## What happens automatically

- Editing your workflow, browsing nodes, and most of the UI never touch the
  remote GPU — only the few actions that actually need one do.
- The remote is only woken up when you queue a prompt, check the queue, or
  (if configured) when something actually needs to upload/view a file and
  no separate CPU endpoint is available.
- If you press **Run** while the GPU is still starting up, each click shows
  up in the **Job Queue** straight away, marked as waiting for a machine, so
  you know it registered (and how many you've queued). You can cancel these
  entries, or use **Clear queue**, before the GPU is ready — that never
  wakes it. They're replaced by the real jobs once the GPU is up, and
  removed again if starting it fails; as a safety net they also vanish by
  themselves after your **Timeout** plus 60 seconds (change
  `QUEUE_PLACEHOLDER_MARGIN` in `proxy/config.py` to adjust that margin).
- Just opening panels like job history or logs never wakes the GPU on its
  own — if nothing's running, you'll see what's saved on your computer (like
  your job history) instead of the proxy reaching out and spinning something
  up just to look.
- Once a job finishes, the proxy keeps things open just long enough for the
  progress bar and final log lines to arrive, then disconnects — unless a
  file you viewed is still being auto-downloaded (it waits for that to
  finish) or you've turned on **Keep GPU warm**.
- Model/checkpoint dropdowns show what your remote machine has, not your
  local files. The list is fetched when you save a URL or turn the proxy on;
  the panel says **Updating model list...** meanwhile, then **Remote model
  list cached (N fields)**. If it says **not yet cached**, the fetch failed
  (see the console log) and the model dropdowns stay empty until it works,
  so you can't pick a model the server might not have. Each list belongs to
  its URL: another server's list is never reused, and a failed refresh
  keeps the last good list for the same URL. With a **Remote
  CPU URL** the list is fetched from that cheaper endpoint; without one the
  GPU is used if it's already up, and woken only when you have no list yet.
  Added models on the server? Use **Refresh Models** — it fetches right away,
  waking the GPU (or the CPU endpoint) if needed. If the dropdowns still look
  stale, reload the ComfyUI browser tab.
- The **Imported** tab of the Media Assets panel lists the files on the
  remote (those you uploaded or imported), not the ones on your computer.
  That list is kept on your computer (in `remote_files.db`, separately for
  each URL) and updated whenever you upload a file, so opening the tab wakes
  nothing. The remote is only asked when there's no saved list yet or it's
  over a day old — your **Remote CPU URL** if you have one, else the GPU if
  it's running; if neither can be asked you keep seeing the saved list (an
  empty one if there never was any). The asset lists below are asked of the
  remote each time.
  With `--enable-assets` on your local ComfyUI the panel uses the asset lists
  instead, with previews and edits too, and the proxy answers all of it from
  the remote.  </br>_Safest:_ when you have a **Remote CPU URL**, turn the assets
  system on only for that CPU container and leave it off on the GPU one;
  after your last job finishes, the proxy then asks the CPU container to
  rescan its output folder once, so new results show up (not after every
  job, since that scan walks the whole folder). If both containers run it on
  one shared user folder, ComfyUI's lock on the database may not hold across
  them (watch for "database is locked" message).
- If your computer has no GPU of its own, the Crystools resource monitor
  (the original, or its MonitorOnly fork) and comfyui-resource-monitor list a
  placeholder one (an NVIDIA L4 with 24 GB by default) so their GPU meters
  still appear; while a job runs they show the remote GPU's real readings —
  for that, the remote needs the same monitor installed. To change the
  placeholder, edit `GPU_NAME`, `GPU_COUNT` and `GPU_VRAM_GB` near the top of
  `proxy/config.py`.
- The proxy's own log lines, and the error messages it shows in the UI,
  replace the host of your remote URLs and your auth keys with placeholders
  such as `<RemoteGPU_Host>` and `<RemoteGPU_AuthKey>`, so they can be pasted
  into a bug report. Skim them anyway: lines from ComfyUI or other extensions
  aren't covered.

## If something seems stuck

Open the panel and use:
- **Clear Stuck State** — tells the proxy to forget about any job or
  connection it thinks is still active, without needing to restart ComfyUI.
  Use this if queuing a new prompt seems to hang or do nothing, or if the
  proxy gave up checking an unresponsive GPU and you want it to try again.
- **Reset to Defaults** — resets every setting above back to its default
  (and turns the proxy off). Asks for confirmation first, since it clears
  your saved URLs and keys too.

The ComfyUI console log explains what the proxy is doing as it happens —
when it's waking the remote, which endpoint a request went to, and why, so
if something looks off that's the first place to check. For example:  
- If you saw HTTP status 5XX in the logs, it means something wrong happened on server side (ie. Comfy server might be crashed or not ready yet).  
- If the log says **Remote GPU got interrupted while having N queued job(s)**, the remote went away mid-job (preempted or scaled down) and those jobs didn't finish.  

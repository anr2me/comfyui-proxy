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
  cheaper endpoint instead of waking the GPU just to serve a file. This
  only helps if it's a genuinely different, separately-running endpoint —
  pointing it at the same URL as your GPU one does nothing useful.
- **Remote CPU Auth Key** *(optional)* — bearer token for the CPU endpoint,
  if it needs one.
- **Timeout** — how long to wait for the remote to respond before giving
  up, in seconds. Serverless GPUs can take a while to cold-start when
  capacity is scarce, so this defaults fairly high (300s). Raise it if you
  see timeout errors right after queuing a prompt.
- **Post-completion delay** — after a job finishes, keeps the live progress
  connection open a few extra seconds so the progress bar and logs finish
  animating smoothly before disconnecting.
- **Job history cache size** — how many recently-viewed job history pages
  to remember locally, so the Media Assets panel can still show something
  useful without needing to contact the remote.
- **Unresponsive-GPU polling cooldown** — if the remote stops responding
  entirely, pauses automatic background checks for this many seconds before
  trying again, so a broken connection doesn't keep your serverless
  instance looking "busy" forever. If your provider takes longer than the
  default (30s) to actually shut down an idle instance, raise this.
- **Keep GPU warm for video/image viewing** *(off by default)* — only
  relevant if you don't have a Remote CPU URL. Normally, once a job
  finishes, the connection to the GPU closes after a few seconds and the
  GPU can shut down — which can cut off a video you're still watching.
  Turning this on keeps that connection open for as long as you keep
  viewing images/videos (up to the **View-activity idle timeout** below),
  so playback doesn't get cut off. This has a real cost, since it keeps
  paying for the GPU while you're watching.
  - **View-activity idle timeout** — how long to keep the connection open
    after the last time you viewed something, before giving up.

## What happens automatically

- Editing your workflow, browsing nodes, and most of the UI never touch the
  remote GPU — only the few actions that actually need one do.
- The remote is only woken up when you queue a prompt, check the queue, or
  (if configured) when something actually needs to upload/view a file and
  no separate CPU endpoint is available.
- Just opening panels like job history or logs never wakes the GPU on its
  own — if nothing's running, you'll see locally-cached information instead
  of the proxy reaching out and spinning something up just to look.
- Once a job finishes, the proxy keeps things open just long enough for the
  progress bar to finish, then disconnects — unless you've turned on the
  video keep-alive option above.
- Model/checkpoint dropdowns are refreshed from the remote once (and
  whenever you change the URL), so you don't see models that only exist on
  your remote machine as available locally, or vice versa. You may need to refresh the ComfyUI browser tab for the models list changes to reflects on dropdowns.

## If something seems stuck

Open the panel and use:
- **Clear Stuck State** — tells the proxy to forget about any job or
  connection it thinks is still active, without needing to restart ComfyUI.
  Use this if queuing a new prompt seems to hang or do nothing.
- **Reset to Defaults** — resets every setting above back to its default
  (and turns the proxy off). Asks for confirmation first, since it clears
  your saved URLs and keys too.

The ComfyUI console log explains what the proxy is doing as it happens —
when it's waking the remote, which endpoint a request went to, and why, so
if something looks off that's the first place to check.  
If you saw HTTP status 5XX in the logs, it means something wrong happened on server side (ie. Comfy server might be crashed or not ready yet).

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ffmpeg = audio stream ke liye, gcc = tgcrypto build fallback
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# YouTube ke liye yt-dlp ko JavaScript runtime (deno) chahiye
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py media.py db.py panel.py errors.py rebuild_cache.py songs.csv ./
COPY assets ./assets

# Har restart par yt-dlp update (YouTube roz badalta hai). Update fail ho to bhi bot start hoga.
# `exec` zaroori hai: tabhi redeploy ka SIGTERM python tak pahunchta hai (graceful shutdown message).
CMD ["sh", "-c", "pip install -U -q 'yt-dlp[default]' || true; exec python main.py"]

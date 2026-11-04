FROM python:3.11-slim

# ffmpeg = audio stream ke liye, gcc = tgcrypto build fallback
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# YouTube ke liye yt-dlp ko JavaScript runtime (deno) chahiye
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

WORKDIR /app
COPY requirements_4-11.txt .
RUN pip install --no-cache-dir -r requirements_4-11.txt

COPY main.py db.py panel.py songs.csv ./

# Har restart par yt-dlp update (YouTube roz badalta hai), fir bot start
CMD ["sh", "-c", "pip install -U -q 'yt-dlp[default]'; python main.py"]

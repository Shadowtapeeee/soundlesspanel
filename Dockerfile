# Soundless server: API + Telegram admin bot.
# .env is NOT copied into the image; it is passed at runtime (docker run --env-file .env / compose env_file).
FROM node:24-alpine

RUN apk add --no-cache ffmpeg tzdata

WORKDIR /app
COPY soundless.js ./

ENV NODE_ENV=production \
    DATA_DIR=/data \
    PORT=8080

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD wget -qO- http://127.0.0.1:8080/api/health >/dev/null || exit 1

CMD ["node", "--disable-warning=ExperimentalWarning", "soundless.js"]

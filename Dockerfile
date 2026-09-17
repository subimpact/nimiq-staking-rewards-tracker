FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY tracker/ ./tracker/

RUN useradd --create-home --shell /usr/sbin/nologin tracker \
    && mkdir -p /data \
    && chown -R tracker:tracker /app /data

USER tracker

EXPOSE 8649

VOLUME ["/data"]

CMD ["python", "-m", "tracker.main"]

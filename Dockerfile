FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    ACCEPT_EULA=Y

# Instala dependencias + agrega repo MS con método moderno (signed-by)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl gnupg2 ca-certificates apt-transport-https unixodbc-dev gcc g++; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/microsoft-prod.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends msodbcsql18; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Gunicorn (Railway setea PORT)
CMD exec gunicorn -b 0.0.0.0:${PORT:-5000} app:app --workers=1 --threads=4 --timeout=120

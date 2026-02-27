# ---------- builder ----------
FROM python:3.12-slim AS builder

WORKDIR /install

RUN apt-get update && apt-get install -y \
 gcc \
 g++ \
 libffi-dev \
 libssl-dev \
 default-libmysqlclient-dev \
 pkg-config

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---------- runtime ----------
FROM python:3.12-slim

# Install curl for health check
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /install /usr/local

COPY app ./app

RUN mkdir -p logs uploads

EXPOSE 4001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4001"]
FROM python:3.12.9-slim

WORKDIR /app

# Build deps needed by pyarrow and pyiceberg C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY iceberg_bridge.py .

CMD ["python", "-u", "iceberg_bridge.py"]

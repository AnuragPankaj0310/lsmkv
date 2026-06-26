FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Data volume (WAL + SSTables)
VOLUME ["/app/data"]

# Default: node 0 on port 7001
ENV LSMKV_NODE_INDEX=0

EXPOSE 7001 9001

CMD ["python", "run_server.py"]

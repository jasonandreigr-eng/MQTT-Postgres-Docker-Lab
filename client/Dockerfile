FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
 libpq5 \
&& rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/client.py .
COPY src/wait_for.py .
# Wait for broker + DB, then exec the client
CMD ["python", "-u", "wait_for.py"]
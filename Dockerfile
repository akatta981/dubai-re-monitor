# Dubai RE Monitor — run the dashboard in Docker
#   docker build -t dubai-re-monitor .
#   docker run -p 8501:8501 -v dubai-re-data:/app/data --env-file .env dubai-re-monitor
# then open http://localhost:8501
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# SQLite database + logs live here — mount a volume to keep data between runs
VOLUME ["/app/data"]
EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

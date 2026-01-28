FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python modules
COPY database.py .
COPY database_postgres.py .
COPY db_wrapper.py .
COPY proxy_manager.py .
COPY analytics.py .
COPY notifications.py .
COPY web.py .
COPY app.py .

# Expose web dashboard port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

CMD ["python", "app.py"]

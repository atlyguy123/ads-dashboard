# Use Python 3.9 slim image for consistency
FROM python:3.9.18-slim

# Set working directory
WORKDIR /app

# Install system dependencies (including Node.js for React build)
RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Copy package files first for better Docker layer caching
COPY requirements.txt .
COPY orchestrator/dashboard/client/package*.json ./orchestrator/dashboard/client/

# Install Python dependencies with exact versions
RUN pip install --no-cache-dir -r requirements.txt

# Install Node.js dependencies
RUN cd orchestrator/dashboard/client && npm ci --only=production

# Copy application code
COPY . .

# Build React frontend
RUN cd orchestrator/dashboard/client && \
    REACT_APP_API_URL='' npm run build && \
    cp -r build/* ../static/

# Create database directory for persistent volume mounting
RUN mkdir -p /app/database

# Copy schema file to a safe location (not overwritten by volume mount)
RUN cp /app/database/schema.sql /app/schema.sql

# Set environment variables
ENV FLASK_ENV=production
ENV HOST=0.0.0.0
ENV PYTHONPATH=/app

# Expose port (Railway will set PORT env var)
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${PORT:-5000}/health || exit 1

# Start the application using the launcher script
CMD ["python3", "launcher.py"] 
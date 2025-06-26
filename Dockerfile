# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create database directory for persistent volume mounting
RUN mkdir -p /app/database

# Copy schema file to a safe location (not overwritten by volume mount)
# The volume will mount at /app/database/ and overwrite the copied schema.sql
RUN cp /app/database/schema.sql /app/schema.sql

# Set environment variables
ENV FLASK_ENV=production
ENV HOST=0.0.0.0

# Note: PORT is set dynamically by Railway via environment variable

# Create entrypoint script
RUN echo '#!/bin/bash\ncd orchestrator\npython3 app.py' > /app/start.sh
RUN chmod +x /app/start.sh

# Start the application
CMD ["/app/start.sh"] 
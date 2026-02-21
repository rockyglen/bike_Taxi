# Use the official astral-sh/uv image for fast dependency management
FROM astral-sh/uv:python3.10-bookworm-slim AS builder

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files first for caching
COPY pyproject.toml .
COPY requirements.txt .

# Install dependencies using uv
# --no-install-project is used if we don't have a local package to install
RUN uv pip install --system -r requirements.txt

# Final stage
FROM python:3.10-slim-bookworm

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create a non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose Streamlit's default port
EXPOSE 8501

# Healthcheck to ensure the dashboard is running
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Run the Streamlit dashboard
ENTRYPOINT ["streamlit", "run", "frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

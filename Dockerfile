FROM python:3.13-slim

WORKDIR /app

# Install uv for faster dependency management
RUN pip install uv

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


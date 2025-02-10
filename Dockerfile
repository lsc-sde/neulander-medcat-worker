FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1

# Install git
RUN apt-get update && apt-get install -y git

# Create a non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set the working directory
WORKDIR /home/appuser

# Change ownership of the application directory
RUN chown -R appuser:appuser /home/appuser

# Copy the application code
COPY . /home/appuser

RUN uv add git+https://github.com/lsc-sde/neulander-core && uv sync --no-dev --no-sources --frozen --no-cache

# Switch to the non-root user
USER appuser

EXPOSE 8000

# Run the application.
ENV PATH="/home/appuser/.venv/bin:$PATH"
CMD ["faststream", "run", "src/main:app", "--port", "8000", "--host", "0.0.0.0"]

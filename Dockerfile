FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev
COPY wireiot ./wireiot
ENV PATH="/app/.venv/bin:$PATH"
CMD ["uvicorn", "wireiot.api:app", "--host", "0.0.0.0", "--port", "8000"]

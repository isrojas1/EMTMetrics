FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY pyproject.toml poetry.lock* README.md /app/
COPY ./src /app/src
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main

CMD ["uvicorn", "emtmetrics.app:app", "--host", "0.0.0.0", "--port", "8000"]

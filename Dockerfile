FROM python:3.9-slim

WORKDIR /app

RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

COPY . .

RUN mkdir -p /app/uploads

CMD ["python", "bot.py"]
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -e .

EXPOSE 8000
CMD ["python", "-m", "market_pipeline", "serve", "--host", "0.0.0.0", "--port", "8000"]

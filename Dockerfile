FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CAL_HOST=0.0.0.0 \
    CAL_PORT=8080

WORKDIR /srv/app

# Install dependencies first so the layer is cached across code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080

# Dependencies are installed at image build; initialization (logging, routes)
# runs at process startup via the FastAPI lifespan.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

FROM python:3.11-slim

WORKDIR /app

# Zależności systemowe (potrzebne dla pymysql + openpyxl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Najpierw requirements (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kod aplikacji
COPY . .

# Port domyślny Streamlit — Railway nadpisuje przez $PORT
EXPOSE 8501

# Railway ustawia $PORT automatycznie
CMD streamlit run app.py \
    --server.port=${PORT:-8501} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false

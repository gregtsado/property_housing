FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY property.py .
COPY synthetic_property_data.csv .

CMD ["python", "property.py"]


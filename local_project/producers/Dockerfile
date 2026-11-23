FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2
COPY event_generator.py .
COPY producer.py .
CMD ["python", "-u", "producer.py"]
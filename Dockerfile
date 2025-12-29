FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir faker jsonschema click confluent-kafka

# Copy the app
COPY main.py .

# Run as non-root
USER 1000:1000

ENTRYPOINT ["python", "main.py", "generate"]
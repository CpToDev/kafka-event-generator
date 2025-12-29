import json
import logging
import random
import sys
import time
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, Union

import click
from faker import Faker
import jsonschema
from confluent_kafka import Producer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()


def generate_from_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively generate random data conforming to the JSON schema using Faker."""
    if schema.get("type") == "object":
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        payload = {}
        for prop, prop_schema in properties.items():
            if prop in required or random.random() < 0.8:  # Bias towards required fields
                payload[prop] = generate_from_schema(prop_schema)
        return payload
    elif schema.get("type") == "array":
        items_schema = schema.get("items", {})
        min_items = schema.get("minItems", 0)
        max_items = schema.get("maxItems", 10) or 10
        num_items = random.randint(min_items, max_items)
        return [generate_from_schema(items_schema) for _ in range(num_items)]
    elif schema.get("type") == "string":
        format_ = schema.get("format", "")
        if format_ == "email":
            return fake.email()
        elif format_ == "name":
            return fake.name()
        elif format_ == "date":
            return fake.date().isoformat()
        elif format_ == "date-time":
            return fake.date_time().isoformat()
        else:
            min_len = schema.get("minLength", 1)
            return fake.text(max_nb_chars=min_len + random.randint(0, 50))[:min_len + 50]
    elif schema.get("type") == "integer":
        min_val = schema.get("minimum", -sys.maxsize)
        max_val = schema.get("maximum", sys.maxsize)
        return random.randint(min_val, max_val)
    elif schema.get("type") == "number":
        min_val = schema.get("minimum", -float('inf'))
        max_val = schema.get("maximum", float('inf'))
        return round(random.uniform(min_val, max_val), 2)
    elif schema.get("type") == "boolean":
        return fake.boolean()
    elif schema.get("type") == "null":
        return None
    elif "enum" in schema:
        return random.choice(schema["enum"])
    else:
        return fake.word()  # Fallback


def generate_cdc_event(schema: Dict[str, Any], event_id: int, size: int = 0) -> Dict[str, Any]:
    """Generate a single CDC event: random op, before/after states, optional padding."""
    ts_ms = int(time.time() * 1000)
    op = random.choices(
        ['c', 'u', 'd', 'r'],  # create, update, delete, read
        weights=[0.4, 0.3, 0.2, 0.1],
        k=1
    )[0]

    # Generate after state (always)
    after = generate_from_schema(schema)
    after['id'] = event_id  # Ensure unique ID

    # Generate before state (for update/delete)
    before = None
    if op in ['u', 'd']:
        before = generate_from_schema(schema)
        before['id'] = event_id
        # Simulate change: mutate a random field in after
        if op == 'u':
            keys = list(after.keys())
            if keys:
                key = random.choice(keys)
                if isinstance(after[key], str) and key != 'id':
                    after[key] = fake.word()
                elif isinstance(after[key], int):
                    after[key] += random.randint(1, 10)
                elif isinstance(after[key], float):
                    after[key] += random.uniform(1, 10)
                elif isinstance(after[key], bool):
                    after[key] = not after[key]

    payload = {
        "before": before,
        "after": after if op != 'd' else None,
        "op": op,
        "ts_ms": ts_ms,
        "source": {
            "version": "1.0",
            "connector": "faker-cdc",
            "ts_ms": ts_ms,
            "snapshot": op == 'r'
        }
    }

    # Schema reference (simplified)
    event = {
        "schema": {"type": "struct", "fields": [{"name": k, "type": str(type(v).__name__)} for k, v in
                                                schema.get("properties", {}).items()]},
        "payload": payload
    }

    # Pad if needed
    if size > 0:
        event = pad_event(event, size)

    return event


def pad_event(event: Dict[str, Any], target_size: int) -> Dict[str, Any]:
    """Pad the event payload with a 'padding' field to approximate target JSON size."""
    padded = deepcopy(event)
    current_bytes = json.dumps(padded).encode('utf-8')
    current_size = len(current_bytes)

    pad_len = max(0, target_size - current_size)
    if pad_len > 0:
        padding = fake.text(length=pad_len)
        if 'payload' in padded and 'after' in padded['payload']:
            padded['payload']['after']['padding'] = padding
        else:
            padded['payload']['padding'] = padding
        logger.debug(f"Padded event to ~{target_size} bytes (actual: {len(json.dumps(padded).encode('utf-8'))})")

    return padded


def delivery_callback(err, msg):
    """Kafka delivery callback."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


@click.command()
@click.option('--schema-file', '-s', required=True, help='JSON schema file for CDC data')
@click.option('--brokers', '-b',
              help='Kafka bootstrap servers (e.g., localhost:9092). If provided, sends to Kafka topic.')
@click.option('--topic', '-t', default='cdc-topic', help='Kafka topic (default: cdc-topic)')
@click.option('--count', '-c', type=int, required=True, help='Total number of events to generate')
@click.option('--rate', type=int, default=0, help='Events per second (0 = as fast as possible)')
@click.option('--size', type=int, default=0, help='Approximate event size in bytes (pads if >0)')
@click.option('--output', '-o', type=click.Path(), help='Output JSONL file (default: stdout if no Kafka)')
def generate(schema_file: str, brokers: str, topic: str, count: int, rate: int, size: int, output: str):
    """Generate and send JSON CDC events based on schema using Faker."""
    # Load schema
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    if schema.get("type") != "object":
        click.echo("Error: Schema must be an object type", err=True)
        sys.exit(1)

    use_kafka = bool(brokers)
    if use_kafka:
        click.echo(f"Sending {count} CDC events to Kafka ({brokers}) topic {topic}")
    else:
        click.echo(f"Generating {count} CDC events to {'file' if output else 'stdout'}: {schema_file}")

    # Output setup
    if use_kafka:
        producer_conf = {
            'bootstrap.servers': brokers,
            'client.id': 'cdc-generator',
            'acks': 'all',
            'enable.idempotence': True,
            'linger.ms': 5,
            'batch.size': 16384,
            'compression.type': 'snappy',
        }
        producer = Producer(producer_conf)
        send_fn = lambda event: producer.produce(
            topic=topic,
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_callback
        )
    else:
        if output:
            out_file = open(output, 'w')
            writer = lambda event: out_file.write(json.dumps(event) + '\n')
        else:
            writer = lambda event: click.echo(json.dumps(event))
        send_fn = writer

    # Generation loop
    interval = 1.0 / rate if rate > 0 else 0
    start_time = time.time()
    for i in range(count):
        event = generate_cdc_event(schema, i + 1, size)
        send_fn(event)

        if use_kafka:
            producer.poll(0)  # Handle callbacks

        if rate > 0:
            time.sleep(interval)

    if use_kafka:
        producer.flush()
        click.echo("Kafka producer flushed.")
    elif output:
        out_file.close()

    duration = time.time() - start_time
    rate_achieved = count / duration if duration > 0 else 0
    click.echo(f"Generated {count} events in {duration:.2f}s ({rate_achieved:.1f} events/sec)")


if __name__ == '__main__':
    generate()
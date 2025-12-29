Run locally using dockerfile

```commandline
podman run --rm -v $(pwd)/user_schema.json:/app/schema.json cdc-generator -s /app/schema.json -c 10 --brokers localhost:9092 -t cdc-topic --rate 5 --size 1024
```

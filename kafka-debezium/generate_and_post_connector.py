import json
import os
import sys

import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # Debezium runs INSIDE Docker: it must reach Postgres by its service name and
        # internal port, not by the localhost:5432 used by scripts on the host.
        "database.hostname": os.getenv("DEBEZIUM_DB_HOST", "postgres"),
        "database.port": os.getenv("DEBEZIUM_DB_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        # Plain JSON rows without the embedded schema (~10x smaller messages)
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    },
}

# -----------------------------
# Create or update the connector (PUT .../config is idempotent)
# -----------------------------
name = connector_config["name"]
connect_url = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
url = f"{connect_url}/connectors/{name}/config"
headers = {"Content-Type": "application/json"}

response = requests.put(url, headers=headers, data=json.dumps(connector_config["config"]))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("Connector created successfully!")
elif response.status_code == 200:
    print("Connector updated successfully!")
else:
    print(f"Failed to create/update connector ({response.status_code}): {response.text}")
    sys.exit(1)

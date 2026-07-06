import os
import json
import psycopg2
import paho.mqtt.client as mqtt

# --- Config from env ---
MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.environ["MQTT_PORT"])
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
MQTT_TOPIC = os.environ["MQTT_TOPIC"]

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def table_name_for_topic(topic: str) -> str:
    return topic.strip("/").replace("/", "_")


def ensure_table(cur, table: str):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            id SERIAL PRIMARY KEY,
            temp DOUBLE PRECISION,
            received_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def insert_reading(cur, table: str, payload: dict):
    cur.execute(
        f'INSERT INTO "{table}" (temp) VALUES (%s)',
        (payload.get("temp"),)
    )


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[mqtt] connected rc={rc}, subscribing to {MQTT_TOPIC}")
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    topic = msg.topic
    table = table_name_for_topic(topic)

    try:
        payload = json.loads(msg.payload.decode())
    except json.JSONDecodeError:
        print(f"[warn] non-JSON payload on {topic}: {msg.payload!r}")
        return

    print(f"[mqtt] {topic} -> {payload}")

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        conn.autocommit = True
        with conn:
            with conn.cursor() as cur:
                ensure_table(cur, table)
                insert_reading(cur, table, payload)
        print(f"[db] stored in table '{table}'")
    except Exception as e:
        print(f"[db error] {e}")


def main():
    client = mqtt.Client(client_id="pg-client", protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as e:
            print(f"[connect error] {e}, retrying in 5s...")
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()
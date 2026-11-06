import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)

TOPIC = "transactions"
BROKER = os.getenv("KAFKA_SERVERS", "127.0.0.1:23292")
INTERVAL = float(os.getenv("EVENT_INTERVAL", "1.5"))
SOURCES = ["mobile", "web", "pos"]
USERS = ["U10001", "U10002", "U10003", "U10004", "U10005"]


def buat_transaksi():
    return {
        "user_id": random.choice(USERS),
        "amount": random.randint(10000, 500000),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": random.choice(SOURCES),
    }


def main():
    producer = None
    contoh_valid = None
    nomor = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        while True:
            nomor += 1
            data = buat_transaksi()
            jenis = "valid"

            if nomor == 2:
                contoh_valid = data.copy()
            elif nomor == 6:
                data["amount"] = -5000
                jenis = "invalid_amount"
            elif nomor == 7:
                data["amount"] = 15000000
                jenis = "invalid_range"
            elif nomor == 8:
                data["source"] = "app"
                jenis = "invalid_source"
            elif nomor == 9:
                data["timestamp"] = "salah-tanggal"
                jenis = "invalid_timestamp"
            elif nomor == 10:
                data["amount"] = "bukan_angka"
                jenis = "invalid_type"
            elif nomor in (11, 12, 13):
                data["timestamp"] = (
                    datetime.now(timezone.utc) - timedelta(minutes=5)
                ).isoformat(timespec="seconds").replace("+00:00", "Z")
                jenis = "late_event"
            elif nomor == 14 and contoh_valid is not None:
                data = contoh_valid.copy()
                jenis = "duplicate"
            elif nomor == 15:
                data["user_id"] = None
                jenis = "invalid_mandatory"

            producer.send(TOPIC, key=str(data["user_id"]), value=data).get(timeout=15)
            logging.info("Event %s | %s | %s", nomor, jenis, data)
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logging.info("Producer dihentikan")
    except Exception:
        logging.exception("Producer error")
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()

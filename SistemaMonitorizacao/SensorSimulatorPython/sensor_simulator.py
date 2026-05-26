import argparse
import random
import signal
import sys
import time
from datetime import datetime

try:
    import pika
except ImportError:
    print("Modulo 'pika' nao encontrado.")
    print("Instala as dependencias com: pip install -r requirements.txt")
    sys.exit(1)


EXCHANGE_NAME = "sensores_exchange"

DEFAULT_TYPES = {
    "TEMP": ("C", 18.0, 38.0),
    "HUM": ("%", 35.0, 95.0),
    "CO2": ("ppm", 400.0, 2200.0),
    "PM25": ("ug/m3", 5.0, 45.0),
    "PM10": ("ug/m3", 10.0, 90.0),
    "PRESS": ("hPa", 980.0, 1035.0),
    "RUIDO": ("dB", 35.0, 90.0),
    "AR": ("AQI", 20.0, 170.0),
    "LUZ": ("lux", 80.0, 1200.0),
}

running = True


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def routing_key(zona: str, tipo: str) -> str:
    return f"{zona}.{tipo}".lower()


def publish(channel, zona: str, tipo: str, message: str) -> None:
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key(zona, tipo),
        body=message.encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[PY-SENSOR] Publicado em '{routing_key(zona, tipo)}': {message}")


def publish_heartbeat(channel, sensor_id: str, zona: str) -> None:
    timestamp = now_iso()
    message = f"HEARTBEAT|{sensor_id}|{zona}|{timestamp}"
    publish(channel, zona, "heartbeat", message)


def publish_bye(channel, sensor_id: str, zona: str) -> None:
    timestamp = now_iso()
    message = f"BYE|{sensor_id}|{zona}|{timestamp}"
    publish(channel, zona, "control", message)


def publish_data(channel, sensor_id: str, zona: str, tipo: str) -> None:
    unidade, minimum, maximum = DEFAULT_TYPES[tipo]
    valor = minimum + random.random() * (maximum - minimum)
    timestamp = now_iso()
    message = f"DATA|{sensor_id}|{zona}|{timestamp}|{tipo}|{valor:.2f}|{unidade}"
    publish(channel, zona, tipo, message)


def stop(_signum, _frame) -> None:
    global running
    running = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sensor simulator em Python para RabbitMQ.")
    parser.add_argument("--sensor-id", default="S102")
    parser.add_argument("--zona", default="ZONA_ESCOLAR")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--count", type=int, default=0, help="0 = infinito")
    parser.add_argument(
        "--types",
        default="TEMP,HUM,CO2,PM25,PM10,PRESS,RUIDO,AR,LUZ",
        help="Lista separada por virgulas. Ex: TEMP,HUM,CO2",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected_types = [item.strip().upper() for item in args.types.split(",") if item.strip()]

    invalid = [tipo for tipo in selected_types if tipo not in DEFAULT_TYPES]
    if invalid:
        print("Tipos invalidos: " + ", ".join(invalid))
        print("Tipos validos: " + ", ".join(DEFAULT_TYPES.keys()))
        return 2

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.host))
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="topic",
        durable=True,
        auto_delete=False,
    )

    print("[PY-SENSOR] Ligado ao RabbitMQ.")
    print(f"[PY-SENSOR] ID: {args.sensor_id} | Zona: {args.zona}")
    print(f"[PY-SENSOR] Tipos: {', '.join(selected_types)}")
    print(f"[PY-SENSOR] Intervalo: {args.interval}s")

    sent = 0
    try:
        while running and (args.count <= 0 or sent < args.count):
            tipo = random.choice(selected_types)
            publish_data(channel, args.sensor_id, args.zona, tipo)
            publish_heartbeat(channel, args.sensor_id, args.zona)
            sent += 1
            time.sleep(max(0.1, args.interval))
    finally:
        if connection.is_open:
            publish_bye(channel, args.sensor_id, args.zona)
            connection.close()

    print("[PY-SENSOR] Programa terminado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

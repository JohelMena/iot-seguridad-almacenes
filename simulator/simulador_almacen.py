
import os
import json
import time
import uuid
import random
import argparse

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Cargar configuracion
ruta_env = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ruta_env)

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "almacen/seguridad/eventos")

ESCENARIOS = [
    "normal",
    "proximidad",
    "sospechoso",
    "intrusion",
    "falso_positivo",
    "laser",
    "incompleto",
    "sucio"
]


def generar_evento(escenario, dias_historicos=0):

    fecha = datetime.now(timezone.utc)

    if dias_historicos > 0:
        fecha -= timedelta(
            seconds=random.uniform(0, dias_historicos * 86400)
        )

    evento = {
        "event_id": str(uuid.uuid4()),
        "device_id": "simulador-01",
        "timestamp": fecha.isoformat(),
        "source": "virtual",
        "warehouse_id": "ALM-01",
        "zone_id": random.choice([
            "ZONA-01",
            "ZONA-02",
            "ZONA-03"
        ]),
        "system_armed": True,
        "motion_detected": False,
        "door_open": False,
        "distance_cm": round(random.uniform(80, 200), 2),
        "laser_detected": False,
        "relay_active": False,
        "event_type": "normal",
        "response_time_ms": None,
        "ground_truth": False,
        "simulation_case": escenario
    }

    if escenario == "proximidad":

        evento["distance_cm"] = round(
            random.uniform(10, 49), 2
        )
        evento["event_type"] = "proximity"

    elif escenario == "sospechoso":

        evento["motion_detected"] = True
        evento["event_type"] = "suspicious"

    elif escenario == "intrusion":

        evento["motion_detected"] = True
        evento["door_open"] = True
        evento["distance_cm"] = round(
            random.uniform(5, 30), 2
        )
        evento["relay_active"] = True
        evento["event_type"] = "intrusion"
        evento["response_time_ms"] = random.randint(80, 900)
        evento["ground_truth"] = True

    elif escenario == "falso_positivo":

        evento["motion_detected"] = True
        evento["door_open"] = True
        evento["relay_active"] = True
        evento["event_type"] = "intrusion"
        evento["response_time_ms"] = random.randint(80, 900)
        evento["ground_truth"] = False

    elif escenario == "laser":

        evento["laser_detected"] = True
        evento["event_type"] = "suspicious"

    elif escenario == "incompleto":

        campo = random.choice([
            "timestamp",
            "zone_id",
            "distance_cm",
            "motion_detected"
        ])

        evento.pop(campo)

    elif escenario == "sucio":

        campo = random.choice([
            "distance_cm",
            "motion_detected",
            "timestamp"
        ])

        if campo == "distance_cm":
            evento[campo] = -999

        elif campo == "motion_detected":
            evento[campo] = "ERROR_SENSOR"

        else:
            evento[campo] = "FECHA_INVALIDA"

    return evento


def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:
        print("Conexion exitosa con MQTT")
    else:
        print(f"Error de conexion: {reason_code}")


# Argumentos de ejecucion
parser = argparse.ArgumentParser()

parser.add_argument("--cantidad", type=int, default=30)
parser.add_argument("--intervalo", type=float, default=1)
parser.add_argument("--historico", type=int, default=0)

args = parser.parse_args()

if args.cantidad < 1 or args.intervalo < 0 or args.historico < 0:
    parser.error("Parametros invalidos")


# Cliente MQTT
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"simulador-{uuid.uuid4().hex[:8]}"
)

client.on_connect = on_connect

try:

    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()

    print("SIMULADOR DE SEGURIDAD INDUSTRIAL")
    print(f"Topico: {MQTT_TOPIC}")
    print(f"Cantidad de eventos: {args.cantidad}")
    print("-----------------------------------")

    for i in range(args.cantidad):

        # Garantizar variedad en los primeros eventos
        if i < len(ESCENARIOS):
            escenario = ESCENARIOS[i]
        else:
            escenario = random.choices(
                ESCENARIOS,
                weights=[40, 15, 15, 10, 5, 5, 5, 5],
                k=1
            )[0]

        evento = generar_evento(
            escenario,
            args.historico
        )

        mensaje = json.dumps(evento)

        resultado = client.publish(
            MQTT_TOPIC,
            mensaje,
            qos=1
        )

        resultado.wait_for_publish(timeout=10)

        if not resultado.is_published():
            raise RuntimeError("No se confirmo la publicacion MQTT")

        print(
            f"[{i + 1}/{args.cantidad}] "
            f"Evento enviado: {escenario}"
        )

        time.sleep(args.intervalo)

    print("-----------------------------------")
    print("Simulacion finalizada correctamente")

except KeyboardInterrupt:

    print("\nSimulacion detenida")

except Exception as error:

    print(f"Error: {error}")

finally:

    client.disconnect()
    client.loop_stop()

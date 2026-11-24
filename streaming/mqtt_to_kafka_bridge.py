
import os
from pathlib import Path
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# Cargar variables de entorno
ruta_env = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ruta_env)

# Configuracion MQTT
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

# Configuracion Kafka
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Productor Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    acks="all",
    retries=5
)

print("Conexion con Kafka establecida")


# Evento de conexion MQTT
def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:

        print("Conexion exitosa con MQTT")

        client.subscribe(MQTT_TOPIC, qos=1)

        print(f"Suscrito al topico: {MQTT_TOPIC}")

    else:

        print(f"Error de conexion MQTT: {reason_code}")


# Evento de recepcion MQTT
def on_message(client, userdata, message):

    try:

        print("\nMensaje recibido desde MQTT:")
        print(message.payload.decode("utf-8", errors="replace"))

        # Enviar datos originales a Kafka
        resultado = producer.send(
            KAFKA_TOPIC,
            value=message.payload
        )

        metadata = resultado.get(timeout=10)

        print("Evento enviado correctamente a Kafka")
        print(f"Topico: {metadata.topic}")
        print(f"Particion: {metadata.partition}")
        print(f"Offset: {metadata.offset}")

    except Exception as error:

        print(f"Error enviando evento a Kafka: {error}")


# Cliente MQTT
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="seguridad_mqtt_kafka_bridge"
)

client.on_connect = on_connect
client.on_message = on_message

try:

    client.connect(MQTT_HOST, MQTT_PORT, 60)

    print("Iniciando puente MQTT-Kafka...")
    print("Esperando eventos...")

    client.loop_forever()

except KeyboardInterrupt:

    print("\nDeteniendo puente MQTT-Kafka...")

finally:

    client.disconnect()
    producer.flush()
    producer.close()


import os
import json
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

# Cargar variables de entorno
ruta_env = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ruta_env)

# Configuracion Kafka
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Configuracion MongoDB Atlas
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

cliente = None
consumer = None

try:

    # Conexion con MongoDB Atlas
    cliente = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    cliente.admin.command("ping")

    db = cliente[MONGO_DATABASE]
    coleccion = db[MONGO_COLLECTION]

    print("Conexion exitosa con MongoDB Atlas")
    print(f"Base de datos: {db.name}")
    print(f"Coleccion: {coleccion.name}")

    # Conexion con Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="seguridad-mongo-consumer",
        max_poll_records=1
    )

    print("Conexion con Kafka establecida")
    print(f"Escuchando topico: {KAFKA_TOPIC}")
    print("Esperando eventos de seguridad...")

    for mensaje in consumer:

        texto = mensaje.value.decode(
            "utf-8", errors="replace"
        )

        # Intentar interpretar el mensaje como JSON
        try:
            datos = json.loads(texto)
            formato = "json"
        except json.JSONDecodeError:
            datos = None
            formato = "texto"

        # Identificador para evitar duplicados
        identificador = (
            f"{mensaje.topic}:"
            f"{mensaje.partition}:"
            f"{mensaje.offset}"
        )

        # Documento para la Staging Area
        documento = {
            "_id": identificador,
            "data": datos,
            "raw_payload": texto,
            "format": formato,
            "kafka_topic": mensaje.topic,
            "kafka_partition": mensaje.partition,
            "kafka_offset": mensaje.offset,
            "received_at": datetime.now(timezone.utc)
        }

        # Almacenar en MongoDB Atlas
        resultado = coleccion.update_one(
            {"_id": identificador},
            {"$setOnInsert": documento},
            upsert=True
        )

        # Confirmar el mensaje despues de almacenarlo
        consumer.commit()

        print("\n------------------------------")
        print("Evento procesado correctamente")
        print(f"Contenido: {texto}")
        print(f"Offset: {mensaje.offset}")
        print(f"ID MongoDB: {identificador}")

        if resultado.upserted_id is not None:
            print("Evento almacenado en MongoDB Atlas")
        else:
            print("Evento existente, duplicado evitado")

except KeyboardInterrupt:

    print("\nDeteniendo consumidor...")

except Exception as error:

    print(f"Error: {error}")

finally:

    if consumer is not None:
        consumer.close()

    if cliente is not None:
        cliente.close()

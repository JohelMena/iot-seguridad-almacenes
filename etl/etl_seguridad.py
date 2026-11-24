
import os
import uuid
import math

from pathlib import Path
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from pymongo import MongoClient
from bson import json_util
import psycopg

# Cargar configuracion
ruta_env = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ruta_env)

COSTA_RICA = timezone(timedelta(hours=-6))

DIAS = [
    "Lunes", "Martes", "Miercoles",
    "Jueves", "Viernes", "Sabado", "Domingo"
]

MESES = [
    "Enero", "Febrero", "Marzo", "Abril",
    "Mayo", "Junio", "Julio", "Agosto",
    "Septiembre", "Octubre", "Noviembre", "Diciembre"
]


# =============================================
# TRANSFORMACION Y VALIDACION
# =============================================

def validar_evento(documento):

    datos = documento.get("data")

    if not isinstance(datos, dict):
        raise ValueError("El documento no contiene un evento JSON")

    campos = [
        "event_id",
        "device_id",
        "timestamp",
        "source",
        "warehouse_id",
        "zone_id",
        "system_armed",
        "motion_detected",
        "door_open",
        "distance_cm",
        "relay_active",
        "event_type"
    ]

    for campo in campos:
        if campo not in datos or datos[campo] is None:
            raise ValueError(f"Campo faltante: {campo}")

    datos = datos.copy()

    # Validar identificador
    try:
        datos["event_id"] = uuid.UUID(str(datos["event_id"]))
    except (ValueError, TypeError):
        raise ValueError("Identificador de evento invalido")

    # Validar campos de texto
    limites = {
        "device_id": 100,
        "warehouse_id": 50,
        "zone_id": 50,
        "source": 20,
        "event_type": 50
    }

    for campo, limite in limites.items():

        valor = datos[campo]

        if not isinstance(valor, str):
            raise ValueError(f"Texto invalido: {campo}")

        valor = valor.strip()

        if not valor or len(valor) > limite:
            raise ValueError(f"Longitud invalida: {campo}")

        datos[campo] = valor

    # Validar origen
    if datos["source"] not in ["physical", "virtual"]:
        raise ValueError("Origen de datos invalido")

    # Validar tipo de evento
    if datos["event_type"] not in [
        "normal", "proximity", "suspicious", "intrusion"
    ]:
        raise ValueError("Tipo de evento desconocido")

    # Validar fecha
    try:
        fecha = datetime.fromisoformat(
            datos["timestamp"]
        )

        if fecha.tzinfo is None:
            raise ValueError()

    except (ValueError, TypeError):
        raise ValueError("Fecha del evento invalida")

    datos["fecha_utc"] = fecha.astimezone(timezone.utc)
    datos["fecha_local"] = fecha.astimezone(COSTA_RICA)

    # Validar distancia
    distancia = datos["distance_cm"]

    if (
        type(distancia) not in (int, float)
        or not math.isfinite(distancia)
        or not 0 <= distancia <= 400
    ):
        raise ValueError("Distancia fuera del rango permitido")

    # Validar campos booleanos
    booleanos = [
        "system_armed",
        "motion_detected",
        "door_open",
        "relay_active"
    ]

    for campo in booleanos:
        if type(datos[campo]) is not bool:
            raise ValueError(f"Valor booleano invalido: {campo}")

    # Validar campos opcionales
    for campo in ["laser_detected", "ground_truth"]:

        valor = datos.get(campo)

        if valor is not None and type(valor) is not bool:
            raise ValueError(f"Valor invalido: {campo}")

    respuesta = datos.get("response_time_ms")

    if respuesta is not None:
        if (
            type(respuesta) is not int
            or not 0 <= respuesta <= 2147483647
        ):
            raise ValueError("Tiempo de respuesta invalido")

    escenario = datos.get("simulation_case")

    if escenario is not None:
        if not isinstance(escenario, str) or len(escenario) > 50:
            raise ValueError("Escenario de simulacion invalido")

    return datos


# =============================================
# CARGAR DIMENSIONES
# =============================================

def cargar_dimensiones(conn, datos):

    fecha = datos["fecha_local"]

    conn.execute("""
        INSERT INTO dw.dim_fecha (
            fecha_id, anio, mes, dia,
            dia_semana, nombre_dia, nombre_mes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fecha_id) DO NOTHING
    """, (
        fecha.date(),
        fecha.year,
        fecha.month,
        fecha.day,
        fecha.isoweekday(),
        DIAS[fecha.weekday()],
        MESES[fecha.month - 1]
    ))

    conn.execute("""
        INSERT INTO dw.dim_dispositivo (
            device_id, source
        )
        VALUES (%s, %s)
        ON CONFLICT (device_id, source) DO NOTHING
    """, (
        datos["device_id"],
        datos["source"]
    ))

    dispositivo_id = conn.execute("""
        SELECT dispositivo_id
        FROM dw.dim_dispositivo
        WHERE device_id = %s AND source = %s
    """, (
        datos["device_id"],
        datos["source"]
    )).fetchone()[0]

    conn.execute("""
        INSERT INTO dw.dim_zona (
            warehouse_id, zone_code
        )
        VALUES (%s, %s)
        ON CONFLICT (warehouse_id, zone_code) DO NOTHING
    """, (
        datos["warehouse_id"],
        datos["zone_id"]
    ))

    zona_id = conn.execute("""
        SELECT zona_id
        FROM dw.dim_zona
        WHERE warehouse_id = %s AND zone_code = %s
    """, (
        datos["warehouse_id"],
        datos["zone_id"]
    )).fetchone()[0]

    tipo_id = conn.execute("""
        SELECT tipo_evento_id
        FROM dw.dim_tipo_evento
        WHERE event_type = %s
    """, (
        datos["event_type"],
    )).fetchone()[0]

    return dispositivo_id, zona_id, tipo_id


# =============================================
# CARGAR TABLA DE HECHOS
# =============================================

def cargar_evento(conn, datos, mongo_id):

    dispositivo_id, zona_id, tipo_id = cargar_dimensiones(
        conn, datos
    )

    resultado = conn.execute("""
        INSERT INTO dw.fact_eventos_seguridad (
            event_id,
            fecha_id,
            dispositivo_id,
            zona_id,
            tipo_evento_id,
            timestamp_evento,
            hora,
            system_armed,
            motion_detected,
            door_open,
            distance_cm,
            laser_detected,
            relay_active,
            response_time_ms,
            ground_truth,
            simulation_case,
            mongo_document_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_id) DO NOTHING
    """, (
        datos["event_id"],
        datos["fecha_local"].date(),
        dispositivo_id,
        zona_id,
        tipo_id,
        datos["fecha_utc"],
        datos["fecha_local"].hour,
        datos["system_armed"],
        datos["motion_detected"],
        datos["door_open"],
        datos["distance_cm"],
        datos.get("laser_detected"),
        datos["relay_active"],
        datos.get("response_time_ms"),
        datos.get("ground_truth"),
        datos.get("simulation_case"),
        mongo_id
    ))

    return resultado.rowcount == 1


# =============================================
# REGISTRAR DATOS RECHAZADOS
# =============================================

def registrar_rechazo(conn, ejecucion_id, documento, motivo):

    original = json_util.dumps(
        documento,
        json_options=json_util.CANONICAL_JSON_OPTIONS
    )

    conn.execute("""
        INSERT INTO etl.etl_rechazos (
            ejecucion_id,
            mongo_document_id,
            datos_originales,
            motivo
        )
        VALUES (%s, %s, %s::jsonb, %s)
        ON CONFLICT (mongo_document_id) DO NOTHING
    """, (
        ejecucion_id,
        str(documento["_id"]),
        original,
        motivo
    ))


# =============================================
# EJECUCION PRINCIPAL DEL ETL
# =============================================

def ejecutar_etl():

    leidos = 0
    insertados = 0
    rechazados = 0
    duplicados = 0

    with MongoClient(
        os.getenv("MONGO_URI"),
        serverSelectionTimeoutMS=10000
    ) as mongo:

        mongo.admin.command("ping")

        coleccion = mongo[
            os.getenv("MONGO_DATABASE")
        ][
            os.getenv("MONGO_COLLECTION")
        ]

        print("Conexion exitosa con MongoDB Atlas")

        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            connect_timeout=10
        ) as conn:

            print("Conexion exitosa con PostgreSQL")

            ejecucion_id = conn.execute("""
                INSERT INTO etl.etl_control (estado)
                VALUES ('EN_PROCESO')
                RETURNING ejecucion_id
            """).fetchone()[0]

            conn.commit()

            print(f"Iniciando ETL: {ejecucion_id}")
            print("--------------------------------")

            try:

                with conn.transaction():

                    # Extraccion por lotes de MongoDB
                    with coleccion.find().batch_size(200) as documentos:

                        for documento in documentos:

                            leidos += 1
                            mongo_id = str(documento["_id"])

                            try:
                                datos = validar_evento(documento)

                            except ValueError as error:

                                registrar_rechazo(
                                    conn,
                                    ejecucion_id,
                                    documento,
                                    str(error)
                                )

                                rechazados += 1

                                print(f"RECHAZADO: {error}")
                                continue

                            if cargar_evento(conn, datos, mongo_id):

                                insertados += 1

                                print(
                                    f"INSERTADO: "
                                    f"{datos['event_type']}"
                                )

                            else:

                                duplicados += 1
                                print(f"DUPLICADO: {mongo_id}")

                    conn.execute("""
                        UPDATE etl.etl_control
                        SET
                            fecha_fin = CURRENT_TIMESTAMP,
                            registros_leidos = %s,
                            registros_insertados = %s,
                            registros_rechazados = %s,
                            registros_duplicados = %s,
                            estado = 'COMPLETADO'
                        WHERE ejecucion_id = %s
                    """, (
                        leidos,
                        insertados,
                        rechazados,
                        duplicados,
                        ejecucion_id
                    ))

            except Exception as error:

                conn.rollback()

                conn.execute("""
                    UPDATE etl.etl_control
                    SET
                        fecha_fin = CURRENT_TIMESTAMP,
                        estado = 'ERROR',
                        registros_leidos = %s,
                        observaciones = %s
                    WHERE ejecucion_id = %s
                """, (
                    leidos,
                    str(error),
                    ejecucion_id
                ))

                conn.commit()
                raise

    print("--------------------------------")
    print("ETL FINALIZADO CORRECTAMENTE")
    print(f"Registros leidos: {leidos}")
    print(f"Registros insertados: {insertados}")
    print(f"Registros rechazados: {rechazados}")
    print(f"Registros duplicados: {duplicados}")


if __name__ == "__main__":

    try:
        ejecutar_etl()

    except Exception as error:
        print(f"Error durante el ETL: {error}")
        raise SystemExit(1)

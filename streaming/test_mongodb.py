
import os
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

ruta_env = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ruta_env)

uri = os.getenv("MONGO_URI")
cliente = None

try:
    if not uri:
        raise ValueError("No se encontro MONGO_URI en el archivo .env")

    cliente = MongoClient(uri, serverSelectionTimeoutMS=10000)

    cliente.admin.command("ping")

    print("Conexion exitosa con MongoDB Atlas")

    db = cliente[os.getenv("MONGO_DATABASE")]
    coleccion = db[os.getenv("MONGO_COLLECTION")]

    print("Base de datos:", db.name)
    print("Coleccion:", coleccion.name)
    print("Documentos:", coleccion.count_documents({}))

except Exception as error:
    print("Error de conexion:", error)

finally:
    if cliente is not None:
        cliente.close()

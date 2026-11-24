"""Configuración y seguridad del pipeline de ingesta.

Carga las credenciales y la API key desde el ``.env`` de la raíz del proyecto.
boto3 lee automáticamente esas variables de ``os.environ``, así que no es
necesario pasarlas explícitamente al crear el cliente de S3.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


def _requerida(nombre: str) -> str:
    """Devuelve una variable obligatoria o falla con un mensaje claro."""
    valor = os.getenv(nombre)
    if not valor:
        raise RuntimeError(
            f"Falta la variable de entorno {nombre}. "
            "Copia .env.example a .env y rellena tus valores reales."
        )
    return valor


AWS_ACCESS_KEY_ID = _requerida("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _requerida("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = _requerida("AWS_SESSION_TOKEN")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "datalake-turismo-bronce-plf")
AEMET_API_KEY = _requerida("AEMET_API_KEY")


PROVINCIA = "Asturias"  
MAX_PLAYAS = 15         
PAUSA_ENTRE_LLAMADAS = 1.5

# Rutas y constantes de la API
ARCHIVO_MITECO = DATA_DIR / "playas.csv"
ARCHIVO_AEMET_CODIGOS = DATA_DIR / "playas_codigos.csv"
KEY_MITECO = "bronce/catalogos/miteco_playas/v1/playas.csv"
KEY_AEMET_CODIGOS = "bronce/catalogos/aemet_codigos/v1/playas_codigos.csv"
PREFIJO_PREDICCIONES = "bronce/meteorologia/aemet_prediccion_playas"
AEMET_API_URL = "https://opendata.aemet.es/opendata/api/prediccion/especifica/playa"


def timestamp_iso(momento: datetime | None = None) -> str:
    """Fecha y hora UTC en formato ISO 8601: ``YYYY-MM-DDTHH:MM:SSZ``."""
    momento = momento or datetime.now(timezone.utc)
    return momento.strftime("%Y-%m-%dT%H:%M:%SZ")


def particion_actual(momento: datetime | None = None) -> str:
    """Partición Hive para la fecha UTC indicada (o la actual)."""
    momento = momento or datetime.now(timezone.utc)
    return f"year={momento.year}/month={momento.month:02d}/day={momento.day:02d}"

"""Orquestador del pipeline de ingesta a la capa Bronce en Amazon S3.

Ejecución desde la raíz del proyecto::

    python src/main.py
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

try:  
    from aemet_client import obtener_prediccion
    from config import (
        ARCHIVO_AEMET_CODIGOS,
        ARCHIVO_MITECO,
        KEY_AEMET_CODIGOS,
        KEY_MITECO,
        MAX_PLAYAS,
        PAUSA_ENTRE_LLAMADAS,
        PREFIJO_PREDICCIONES,
        PROVINCIA,
        S3_BUCKET_NAME,
        particion_actual,
    )
    from s3_manager import (
        asegurar_bucket,
        crear_cliente_s3,
        listar_objetos,
        subir_archivo,
        subir_memoria,
    )
except ImportError:  
    from .aemet_client import obtener_prediccion
    from .config import (
        ARCHIVO_AEMET_CODIGOS,
        ARCHIVO_MITECO,
        KEY_AEMET_CODIGOS,
        KEY_MITECO,
        MAX_PLAYAS,
        PAUSA_ENTRE_LLAMADAS,
        PREFIJO_PREDICCIONES,
        PROVINCIA,
        S3_BUCKET_NAME,
        particion_actual,
    )
    from .s3_manager import (
        asegurar_bucket,
        crear_cliente_s3,
        listar_objetos,
        subir_archivo,
        subir_memoria,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def cargar_catalogos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lee los CSV respetando su separador, codificación y ceros a la izquierda."""
    miteco = pd.read_csv(
        ARCHIVO_MITECO,
        sep=",",
        encoding="utf-8-sig",  
        dtype={"Código_IN": str},
    )
    aemet = pd.read_csv(
        ARCHIVO_AEMET_CODIGOS,
        sep=";",
        encoding="cp1252",
        dtype={"ID_PLAYA": str, "ID_MUNICIPIO": str},
    )
    miteco["Código_IN"] = miteco["Código_IN"].str.zfill(5)
    aemet["ID_PLAYA"] = aemet["ID_PLAYA"].str.zfill(7)
    aemet["ID_MUNICIPIO"] = aemet["ID_MUNICIPIO"].str.zfill(5)
    return miteco, aemet


def seleccionar_id_playas(
    miteco: pd.DataFrame, aemet: pd.DataFrame
) -> list[str]:
    """Cruce por municipio INE y selección de hasta 15 IDs de playa de AEMET."""
    es_provincia = (
        miteco["Provincia"].astype(str).str.strip().str.upper()
        == PROVINCIA.upper()
    )
    codigos_ine = sorted(
        miteco.loc[es_provincia, "Código_IN"].dropna().unique()
    )
    seleccion = aemet.loc[aemet["ID_MUNICIPIO"].isin(codigos_ine)].copy()
    seleccion = (
        seleccion.sort_values("ID_PLAYA").head(MAX_PLAYAS)["ID_PLAYA"].tolist()
    )
    logger.info(
        "Provincia %s: %d playas MITECO, %d municipios INE, %d IDs AEMET seleccionados.",
        PROVINCIA,
        int(es_provincia.sum()),
        len(codigos_ine),
        len(seleccion),
    )
    return [str(id_playa).zfill(7) for id_playa in seleccion]


def main() -> int:
    """Ejecuta el pipeline completo y devuelve el código de salida."""
    logger.info("Iniciando pipeline de ingesta hacia Capa Bronce...")
    subidos: list[str] = []
    sin_prediccion: list[str] = []

    try:
        s3_client = crear_cliente_s3()
        asegurar_bucket(s3_client)

        subidos.append(
            subir_archivo(s3_client, ARCHIVO_MITECO, KEY_MITECO, "MITECO_OpenData")
        )
        subidos.append(
            subir_archivo(
                s3_client,
                ARCHIVO_AEMET_CODIGOS,
                KEY_AEMET_CODIGOS,
                "AEMET_Catalogo",
            )
        )

        miteco, aemet = cargar_catalogos()
        ids_playa = seleccionar_id_playas(miteco, aemet)

        ahora = datetime.now(timezone.utc)
        particion = particion_actual(ahora)
        total = len(ids_playa)
        for indice, id_playa in enumerate(ids_playa, start=1):
            logger.info("[%d/%d] Consultando predicción de %s...", indice, total, id_playa)
            datos = obtener_prediccion(id_playa)
            if datos is None:
                sin_prediccion.append(id_playa)
            else:
                hora = datetime.now(timezone.utc).strftime("%H%M%S")
                key = (
                    f"{PREFIJO_PREDICCIONES}/{particion}/"
                    f"pred_playa_{id_playa}_{hora}.json"
                )
                subidos.append(subir_memoria(s3_client, datos, key, "AEMET_API"))
            if indice < total:
                time.sleep(PAUSA_ENTRE_LLAMADAS)

        logger.info("Resumen: %d objetos subidos a s3://%s/", len(subidos), S3_BUCKET_NAME)
        for key in subidos:
            logger.info("  s3://%s/%s", S3_BUCKET_NAME, key)
        if sin_prediccion:
            logger.warning(
                "Playas sin predicción (%d): %s",
                len(sin_prediccion),
                ", ".join(sin_prediccion),
            )
        logger.info(
            "Total en la capa Bronce: %d objetos.", len(listar_objetos(s3_client))
        )
        return 0

    except (ClientError, BotoCoreError) as exc:
        logger.error("Error de conexión con Amazon S3: %s", exc)
        return 1
    except FileNotFoundError as exc:
        logger.error("No se encontró un catálogo local: %s", exc)
        return 1
    except RuntimeError as exc:
        logger.error("Error de configuración: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

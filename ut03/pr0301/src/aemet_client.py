"""Cliente de la API de AEMET: doble salto y control de errores HTTP."""

from __future__ import annotations

import logging
from typing import Any

import requests

try:  # ejecución directa: python src/main.py
    from config import AEMET_API_KEY, AEMET_API_URL
except ImportError:  # ejecución como paquete: python -m src.main
    from .config import AEMET_API_KEY, AEMET_API_URL

logger = logging.getLogger(__name__)


def obtener_prediccion(
    id_playa: str, api_key: str | None = None, timeout: int = 10
) -> dict | list | None:
    """Devuelve el JSON final de la predicción de una playa, o ``None``.

    Realiza el doble salto: primero pide la URL temporal con la ``api_key``
    y después descarga el JSON real. Los errores (404, 429, timeouts, JSON
    inesperado) se registran como WARNING y no detienen el pipeline.
    """
    id_playa = str(id_playa).zfill(7)
    clave = api_key or AEMET_API_KEY
    if not clave:
        logger.error("No hay API key de AEMET configurada; se omite %s.", id_playa)
        return None

    url = f"{AEMET_API_URL}/{id_playa}"
    try:
        # Paso 1: JSON de control con la URL temporal
        control = requests.get(
            url,
            headers={"api_key": clave, "Accept": "application/json"},
            timeout=timeout,
        )
        if control.status_code == 404:
            logger.warning(
                "La playa %s no tiene predicción disponible en AEMET (404). Saltando...",
                id_playa,
            )
            return None
        if control.status_code == 429:
            logger.warning("Límite de peticiones a AEMET (429) para %s. Saltando...", id_playa)
            return None
        control.raise_for_status()

        informacion: dict[str, Any] = control.json()
        estado = informacion.get("estado")
        if estado != 200:
            logger.warning(
                "AEMET devolvió estado %s para %s: %s",
                estado,
                id_playa,
                informacion.get("descripcion"),
            )
            return None
        url_datos = informacion.get("datos")
        if not url_datos:
            logger.warning("AEMET no devolvió URL de datos para %s.", id_playa)
            return None

        # Paso 2: descarga del JSON real desde la URL temporal
        respuesta = requests.get(url_datos, timeout=timeout)
        if respuesta.status_code in (404, 429):
            logger.warning(
                "Descarga de datos de AEMET con estado %s para %s. Saltando...",
                respuesta.status_code,
                id_playa,
            )
            return None
        respuesta.raise_for_status()
        respuesta.encoding = "ISO-8859-15"  # conserva tildes y eñes
        return respuesta.json()

    except requests.Timeout:
        logger.warning("Timeout al consultar AEMET para %s. Saltando...", id_playa)
    except requests.RequestException as exc:
        logger.warning("Error de red/HTTP con AEMET para %s: %s", id_playa, exc)
    except (ValueError, KeyError, TypeError) as exc:
        logger.warning("Respuesta JSON inesperada de AEMET para %s: %s", id_playa, exc)
    return None

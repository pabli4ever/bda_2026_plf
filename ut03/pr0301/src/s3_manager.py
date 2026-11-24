"""Gestión del Data Lake y trazabilidad de los objetos subidos a S3."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

try:  
    from config import (
        AWS_DEFAULT_REGION,
        S3_BUCKET_NAME,
        timestamp_iso,
    )
except ImportError:  
    from .config import AWS_DEFAULT_REGION, S3_BUCKET_NAME, timestamp_iso

logger = logging.getLogger(__name__)


def crear_cliente_s3():
    """Crea el cliente boto3 usando las credenciales cargadas en ``config``."""
    return boto3.client("s3", region_name=AWS_DEFAULT_REGION)


def asegurar_bucket(s3_client) -> None:
    """Crea el bucket si no existe; es idempotente."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logger.info("El bucket %s ya existe.", S3_BUCKET_NAME)
    except ClientError as exc:
        codigo = exc.response.get("Error", {}).get("Code")
        if codigo not in ("404", "NoSuchBucket", "NotFound"):
            raise  # por ejemplo, un token inválido (400/InvalidToken)
        kwargs: dict[str, Any] = {"Bucket": S3_BUCKET_NAME}
        if AWS_DEFAULT_REGION != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": AWS_DEFAULT_REGION
            }
        s3_client.create_bucket(**kwargs)
        logger.info("Bucket creado: %s", S3_BUCKET_NAME)


def _metadatos(source: str) -> dict[str, str]:
    """Metadatos nativos exigidos para todo objeto de la capa Bronce."""
    return {"source": source, "ingestion_timestamp": timestamp_iso()}


def subir_archivo(
    s3_client, ruta_local: Path | str, key: str, source: str
) -> str:
    """Sube un fichero desde disco con ``upload_file`` y devuelve su key."""
    s3_client.upload_file(
        Filename=str(ruta_local),
        Bucket=S3_BUCKET_NAME,
        Key=key,
        ExtraArgs={"Metadata": _metadatos(source)},
    )
    logger.info("Subido desde disco: s3://%s/%s", S3_BUCKET_NAME, key)
    return key


def subir_memoria(
    s3_client,
    datos: dict | list | str | bytes,
    key: str,
    source: str,
    content_type: str = "application/json",
) -> str:
    """Volca datos en memoria con ``put_object`` y devuelve su key."""
    if isinstance(datos, (dict, list)):
        cuerpo = json.dumps(datos, ensure_ascii=False, indent=2).encode("utf-8")
    elif isinstance(datos, str):
        cuerpo = datos.encode("utf-8")
    else:
        cuerpo = datos

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=cuerpo,
        ContentType=content_type,
        Metadata=_metadatos(source),
    )
    logger.info(
        "Subido desde RAM: s3://%s/%s (%d bytes)", S3_BUCKET_NAME, key, len(cuerpo)
    )
    return key


def listar_objetos(s3_client, prefijo: str = "bronce/") -> list[dict[str, Any]]:
    """Lista los objetos de la capa Bronce (soporta paginación)."""
    paginador = s3_client.get_paginator("list_objects_v2")
    objetos: list[dict[str, Any]] = []
    for pagina in paginador.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefijo):
        objetos.extend(pagina.get("Contents", []))
    return objetos

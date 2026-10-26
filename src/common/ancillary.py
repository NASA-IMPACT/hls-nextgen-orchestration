"""Ancillary (LaSRC LADS) data availability."""

from __future__ import annotations

from typing import TYPE_CHECKING

from common.models import GranuleId

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_LADS_PRODUCTS = ("VJ104ANC", "VNP04ANC")


def check_aux_data(granule_id: GranuleId, aux_bucket: str, s3_client: S3Client) -> bool:
    """Return True if ancillary data exists for *granule_id*'s acquisition date."""
    year = granule_id.begin_datetime.strftime("%Y")
    ydoy = granule_id.begin_datetime.strftime("%Y%j")
    for product in _LADS_PRODUCTS:
        resp = s3_client.list_objects_v2(
            Bucket=aux_bucket,
            Prefix=f"lasrc_aux/LADS/{year}/{product}.A{ydoy}",
            MaxKeys=1,
        )
        if resp.get("KeyCount", 0) > 0:
            return True
    return False

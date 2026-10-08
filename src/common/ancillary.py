"""Ancillary data sources, and whether a date's ancillary data is available.

The source is selected by the ANCILLARY_SOURCE environment variable, so
switching sources is a redeploy. Each source knows its own S3 layout: which
acquisition date a newly landed object serves, and which objects must exist
for a date to be processed.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class AncillarySource(ABC):
    """Where LaSRC's ancillary data comes from."""

    @abstractmethod
    def acquisition_date(self, key: str) -> dt.date | None:
        """The acquisition date a newly landed object's arrival may make ready.

        Returns None for an object that is not part of this source's data.
        """

    @abstractmethod
    def is_available(self, acquisition_date: dt.date, s3_client: S3Client) -> bool:
        """Whether the ancillary data for an acquisition date is available."""


# Matches: lasrc_aux/LADS/YYYY/VJ104ANC.AYYYYDDD or VNP04ANC.AYYYYDDD
_LADS_KEY_RE = re.compile(r"lasrc_aux/LADS/(\d{4})/(?:VJ104ANC|VNP04ANC)\.A(\d{7})")
_LADS_PRODUCTS = ("VJ104ANC", "VNP04ANC")


@dataclass(frozen=True)
class LadsSource(AncillarySource):
    """VIIRS LADS ancillary data (VJ104ANC or VNP04ANC), one file per day."""

    bucket: str

    def acquisition_date(self, key: str) -> dt.date | None:
        match = _LADS_KEY_RE.search(key)
        if match is None:
            return None
        try:
            return dt.datetime.strptime(match.group(2), "%Y%j").date()
        except ValueError:
            return None

    def is_available(self, acquisition_date: dt.date, s3_client: S3Client) -> bool:
        year = acquisition_date.strftime("%Y")
        ydoy = acquisition_date.strftime("%Y%j")
        for product in _LADS_PRODUCTS:
            resp = s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"lasrc_aux/LADS/{year}/{product}.A{ydoy}",
                MaxKeys=1,
            )
            if resp.get("KeyCount", 0) > 0:
                return True
        return False


ANCILLARY_SOURCES: dict[str, Callable[[str], AncillarySource]] = {
    "lads": LadsSource,
}
"""Ancillary source factories, given the aux data bucket, by the name
ANCILLARY_SOURCE selects them with."""


def ancillary_source_from_env() -> AncillarySource:
    """The ancillary source the Lambda environment selects.

    Reads ANCILLARY_SOURCE (default "lads") and AUX_DATA_BUCKET_NAME.
    """
    name = os.environ.get("ANCILLARY_SOURCE", "lads")
    try:
        source = ANCILLARY_SOURCES[name]
    except KeyError:
        raise ValueError(
            f"Unknown ANCILLARY_SOURCE {name!r}; choose from {sorted(ANCILLARY_SOURCES)}"
        ) from None
    return source(os.environ["AUX_DATA_BUCKET_NAME"])

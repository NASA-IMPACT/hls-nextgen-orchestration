from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import KeyValuePairTypeDef

EXIT_CODE_LOW_SUN_ANGLE = 3
EXIT_CODE_CLOUDY = 4

HLS_GRANULE_ID_STRFTIME = "%Y%jT%H%M%S"


def convert_safe_id_to_hls_id(safe_id: str) -> str:
    """Convert a Sentinel-2 SAFE ID to an HLS S30 granule ID."""
    parts = safe_id.split("_")
    date_str = parts[2][:15]
    acq_dt = dt.datetime.strptime(date_str[:8], "%Y%m%d")
    doy = f"{acq_dt.timetuple().tm_yday:03d}"
    return f"HLS.S30.{parts[5]}.{acq_dt.year}{doy}{date_str[8:15]}.v2.0"


@dataclass
class GranuleId:
    """Granule identifier"""

    product: str  # Should be "HLS"
    platform: str  # Should be one of ["L30", "S30"]
    tile: str
    begin_datetime: dt.datetime
    version: str  # should be "v2.0"

    @classmethod
    def from_str(cls, granule_id: str) -> GranuleId:
        """Parse components from a string ID"""
        product, platform, tile, begin_datetime, version_major, version_minor = (
            granule_id.split(".")
        )
        return cls(
            product=product,
            platform=platform,
            tile=tile,
            begin_datetime=dt.datetime.strptime(
                begin_datetime, HLS_GRANULE_ID_STRFTIME
            ),
            version=".".join([version_major, version_minor]),
        )

    def __str__(self) -> str:
        """Recombine parts into an ID string"""
        return ".".join(
            [
                self.product,
                self.platform,
                self.tile,
                self.begin_datetime.strftime(HLS_GRANULE_ID_STRFTIME),
                self.version,
            ]
        )

    def doy(self) -> str:
        return self.begin_datetime.strftime(HLS_GRANULE_ID_STRFTIME)


@dataclass(frozen=True)
class GranuleProcessingEvent:
    """The container environment for a granule processing job"""

    workflow: str
    acquisition_date: str  # YYYY-MM-DD
    source_granule_ids: list[str] = field(default_factory=list)
    output_granule_id: str = ""
    attempt: int = 1
    debug_bucket: str | None = None

    def to_envvar(self) -> dict[str, str]:
        """Convert this event to environment variables"""
        envvars: dict[str, str] = {
            "WORKFLOW": self.workflow,
            "ACQUISITION_DATE": self.acquisition_date,
            "SOURCE_GRANULE_IDS": ",".join(self.source_granule_ids),
            "OUTPUT_GRANULE_ID": self.output_granule_id,
            "ATTEMPT": str(self.attempt),
        }
        if self.debug_bucket:
            envvars["DEBUG_BUCKET"] = self.debug_bucket
        return envvars

    def to_environment(self) -> list[KeyValuePairTypeDef]:
        """Format as a container environment definition"""
        return [
            {"name": key, "value": value} for key, value in self.to_envvar().items()
        ]

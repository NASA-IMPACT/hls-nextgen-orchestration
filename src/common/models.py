from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass
from enum import Enum, auto, unique
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import KeyValuePairTypeDef


@unique
class ProcessingOutcome(Enum):
    """Potential outcomes for granule processing

    Individual jobs may succeed or fail in various ways, but ultimately
    the processing of a granule may only be a success or failure.
    """

    SUCCESS = auto()
    FAILURE = auto()


@unique
class JobOutcome(Enum):
    """Potential outcomes for a compute job (e.g., with AWS Batch)

    This enum describes a job attempt of processing a granule. Failures of a
    job may be retryable due to infrastructure or ephemeral issues.
    """

    SUCCESS = auto(), ProcessingOutcome.SUCCESS
    FAILURE_RETRYABLE = auto(), ProcessingOutcome.FAILURE
    FAILURE_NONRETRYABLE = auto(), ProcessingOutcome.FAILURE

    @property
    def processing_outcome(self) -> ProcessingOutcome:
        """The ProcessingOutcome for this job outcome"""
        return self.value[1]


HLS_GRANULE_ID_STRFTIME = "%Y%jT%H%M%S"


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


@dataclass(frozen=True)
class GranuleProcessingEvent:
    """Event message for granule processing jobs"""

    granule_id: str
    attempt: int = 0
    # Events _may_ contain a reference to a debug bucket if the job was
    # submitted in debug mode.
    debug_bucket: str | None = None

    def new_attempt(self) -> GranuleProcessingEvent:
        """Return a new GranuleProcessingEvent for another attempt"""
        return GranuleProcessingEvent(
            granule_id=self.granule_id,
            attempt=self.attempt + 1,
            debug_bucket=self.debug_bucket,
        )

    def to_envvar(self) -> dict[str, str]:
        """Convert this event to environment variable"""
        envvars = {
            "GRANULE_ID": self.granule_id,
            "ATTEMPT": str(self.attempt),
        }
        if self.debug_bucket:
            envvars["DEBUG_BUCKET"] = self.debug_bucket
        return envvars

    @classmethod
    def from_envvar(cls, env: dict[str, str]) -> GranuleProcessingEvent:
        """Parse from provided environment variables

        Raises
        ------
        KeyError
            Raised if the expected keys aren't in the envvars provided
        """
        return cls(
            granule_id=env["GRANULE_ID"],
            attempt=int(env["ATTEMPT"]),
            debug_bucket=env.get("DEBUG_BUCKET"),
        )

    def to_environment(self) -> list[KeyValuePairTypeDef]:
        """Format as a container environment definition"""
        return [
            {"name": key, "value": value} for key, value in self.to_envvar().items()
        ]

    @classmethod
    def from_json(cls, json_str: str) -> GranuleProcessingEvent:
        """Load from a JSON string"""
        data = json.loads(json_str)
        return cls(
            granule_id=data["granule_id"],
            attempt=data["attempt"],
            debug_bucket=data.get("debug_bucket"),
        )

    def to_json(self) -> str:
        """Dump to a JSON string"""
        return json.dumps(asdict(self))

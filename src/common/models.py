from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass, field
from enum import Enum, auto, unique
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import KeyValuePairTypeDef

EXIT_CODE_LOW_SUN_ANGLE = 3
EXIT_CODE_CLOUDY = 4

TERMINAL_STATES: frozenset[ProcessingState]


@unique
class ProcessingState(Enum):
    """Potential state for granule processing"""

    SUCCESS = auto()
    CLOUDY = auto()
    LOW_SUN_ANGLE = auto()
    FAILURE_RETRYABLE = auto()
    FAILURE_NONRETRYABLE = auto()
    AWAITING = auto()
    SUBMITTED = auto()

    def is_terminal(self) -> bool:
        return self in (
            ProcessingState.SUCCESS,
            ProcessingState.CLOUDY,
            ProcessingState.LOW_SUN_ANGLE,
            ProcessingState.FAILURE_NONRETRYABLE,
        )


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

    def doy(self) -> str:
        return self.begin_datetime.strftime(HLS_GRANULE_ID_STRFTIME)


@dataclass(frozen=True)
class GranuleProcessingEvent:
    """Event message for granule processing jobs"""

    workflow: str
    acquisition_date: str  # YYYY-MM-DD
    source_granule_ids: list[str] = field(default_factory=list)
    output_granule_id: str = ""
    attempt: int = 0
    debug_bucket: str | None = None

    def new_attempt(self) -> GranuleProcessingEvent:
        """Return a new GranuleProcessingEvent for another attempt"""
        return GranuleProcessingEvent(
            workflow=self.workflow,
            acquisition_date=self.acquisition_date,
            source_granule_ids=list(self.source_granule_ids),
            output_granule_id=self.output_granule_id,
            attempt=self.attempt + 1,
            debug_bucket=self.debug_bucket,
        )

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

    @classmethod
    def from_envvar(cls, env: dict[str, str]) -> GranuleProcessingEvent:
        """Parse from provided environment variables

        Raises
        ------
        KeyError
            Raised if the expected keys aren't in the envvars provided
        """
        return cls(
            workflow=env["WORKFLOW"],
            acquisition_date=env["ACQUISITION_DATE"],
            source_granule_ids=env["SOURCE_GRANULE_IDS"].split(","),
            output_granule_id=env["OUTPUT_GRANULE_ID"],
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
            workflow=data["workflow"],
            acquisition_date=data["acquisition_date"],
            source_granule_ids=data["source_granule_ids"],
            output_granule_id=data["output_granule_id"],
            attempt=data["attempt"],
            debug_bucket=data.get("debug_bucket"),
        )

    def to_json(self) -> str:
        """Dump to a JSON string"""
        return json.dumps(asdict(self))

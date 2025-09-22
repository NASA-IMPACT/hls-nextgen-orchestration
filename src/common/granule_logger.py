"""Log granule processing event information"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from boto_session_manager import BotoSesManager
from botocore.exceptions import ClientError
from s3pathlib import S3Path

from common.aws_batch import (
    JobDetails,
)
from common.models import (
    GranuleId,
    GranuleProcessingEvent,
    JobOutcome,
    ProcessingOutcome,
)

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import (
        JobDetailTypeDef,
    )


class NoSuchEventAttemptExists(FileNotFoundError):
    """Raised if the logs for the GranuleProcessingEvent doesn't exist"""


@dataclass
class GranuleEventJobLog:
    """HLS-VI processing job attempt details"""

    granule_id: str
    attempt: int
    outcome: JobOutcome
    job_info: JobDetailTypeDef

    def to_json(self) -> str:
        """Export to JSON (enum dumped by name)"""
        return json.dumps(
            {
                "granule_id": self.granule_id,
                "attempt": self.attempt,
                "outcome": self.outcome.name,
                "job_info": self.job_info,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> GranuleEventJobLog:
        """Load from JSON"""
        data = json.loads(json_str)
        return cls(
            granule_id=data["granule_id"],
            attempt=data["attempt"],
            outcome=JobOutcome[data["outcome"]],
            job_info=data["job_info"],
        )


@dataclass
class GranuleLoggerService:
    """Log granule processing details

    The granule logger describes outcomes from "granule processing events"
    by using S3 as a store for logs and outcome breadcrumbs. In order to
    predictably list or search for information about our granule processing
    system this organizes log information into a set of prefixes based on
    information:

    ```
    ./logs/outcome={outcome}/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/
    ```

    This organizes logs by outcome ("success" or "failure") first as we anticipate
    wanting to search and analyze jobs that have "failed" more than the successes.

    Within each prefix job attempts are organized by the attempt. For example:

    ```
    ./logs/outcome=failure/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/attempt=1.json
    ./logs/outcome=success/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/attempt=2.json
    ```

    When a successful attempt has been logged, any previous attempts that had failures
    are removed from the failure prefix and reorganized into the "success" prefix to
    help prune the prefix containing failures.
    """

    bucket: str
    logs_prefix: str
    bsm: BotoSesManager = field(default_factory=BotoSesManager)

    # mapping of ProcessingOutcome to S3 path component
    outcome_to_prefix: ClassVar[dict[ProcessingOutcome, str]] = {
        ProcessingOutcome.SUCCESS: "success",
        ProcessingOutcome.FAILURE: "failure",
    }
    prefix_to_outcome: ClassVar[dict[str, ProcessingOutcome]] = {
        value: key for key, value in outcome_to_prefix.items()
    }

    # regex to parse the log object into components
    log_path_regex: ClassVar[re.Pattern[str]] = re.compile(
        "/".join(
            [
                r"outcome=(?P<outcome>\w+)",
                r"platform=(?P<platform>\w+)",
                r"acquisition_date=(?P<acquisition_date>[\d-]+)",
                r"granule_id=(?P<granule_id>[\w\.]+)",
                r"attempt=(?P<attempt>[0-9]+)\.json$",
            ]
        )
    )
    # regex to match on attempt object name
    attempt_log_regex: ClassVar[re.Pattern[str]] = re.compile(r"^attempt=[0-9]+\.json$")

    def _prefix_for_granule_id_outcome(
        self, granule_id: GranuleId, outcome: ProcessingOutcome
    ) -> S3Path:
        """Return the S3 path for storing this granule's info"""
        date = granule_id.begin_datetime.strftime("%Y-%m-%d")
        return S3Path(
            self.bucket,
            self.logs_prefix.rstrip("/"),
            f"outcome={self.outcome_to_prefix[outcome]}",
            f"platform={granule_id.platform}",
            f"acquisition_date={date}",
            f"granule_id={str(granule_id)}",
        )

    def _path_for_event_outcome(
        self,
        event: GranuleProcessingEvent,
        outcome: ProcessingOutcome,
    ) -> S3Path:
        granule_id = GranuleId.from_str(event.granule_id)
        prefix = self._prefix_for_granule_id_outcome(granule_id, outcome)
        return S3Path(prefix, f"attempt={event.attempt}.json")

    def _path_to_event_outcome(
        self,
        log_artifact: S3Path,
    ) -> tuple[GranuleProcessingEvent, ProcessingOutcome]:
        """Determine an event info from a log artifact path

        This is the inverse of the `_path_for_event_outcome`
        """
        path = log_artifact.key.removeprefix(self.logs_prefix).lstrip("/")
        match = self.log_path_regex.match(path)
        if not match:
            raise ValueError(
                f"Cannot parse {log_artifact.uri} into a GranuleProcessingEvent"
            )

        granule_id = match.group("granule_id")
        attempt = int(match.group("attempt"))
        outcome = self.prefix_to_outcome[match.group("outcome")]

        return (
            GranuleProcessingEvent(granule_id, attempt),
            outcome,
        )

    def _filter_attempt_log(self, path: S3Path) -> bool:
        return bool(self.attempt_log_regex.match(path.basename))

    def _list_logs_for_outcome(
        self, granule_id: str, outcome: ProcessingOutcome
    ) -> list[S3Path]:
        """Helper function to find logs for some outcome"""
        prefix = self._prefix_for_granule_id_outcome(
            GranuleId.from_str(granule_id), outcome
        )
        paths = []
        for path in prefix.iter_objects(bsm=self.bsm).filter(self._filter_attempt_log):
            paths.append(path)
        return paths

    def _clean_failures(self, granule_id: str) -> None:
        """Cleanup failures"""
        for failure_path in self._list_logs_for_outcome(
            granule_id, ProcessingOutcome.FAILURE
        ):
            event, outcome = self._path_to_event_outcome(failure_path)
            success_path = self._path_for_event_outcome(
                event, ProcessingOutcome.SUCCESS
            )
            failure_path.copy_to(success_path, bsm=self.bsm, overwrite=True)
            failure_path.delete(bsm=self.bsm)

    def put_event_details(self, details: JobDetails) -> None:
        """Log event details"""
        event = details.get_granule_event()
        job_outcome = details.get_job_outcome()
        s3path = self._path_for_event_outcome(event, job_outcome.processing_outcome)
        event_log = GranuleEventJobLog(
            granule_id=event.granule_id,
            attempt=event.attempt,
            outcome=job_outcome,
            job_info=details.get_job_info(),
        )
        s3path.write_text(event_log.to_json(), bsm=self.bsm)
        if job_outcome.processing_outcome == ProcessingOutcome.SUCCESS:
            self._clean_failures(event.granule_id)

    def get_event_details(self, event: GranuleProcessingEvent) -> JobDetails:
        """Get event details for an event

        Raises
        ------
        NoSuchEventAttemptExists
            Raised if the event provided doesn't exist in the logs
        """
        for outcome in ProcessingOutcome:
            path = self._path_for_event_outcome(event, outcome)
            try:
                data = path.read_text(bsm=self.bsm)
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise
            else:
                event_log = GranuleEventJobLog.from_json(data)
                return JobDetails(event_log.job_info)

        raise NoSuchEventAttemptExists(f"Cannot find logs for {event}")

    def list_events(
        self, granule_id: str | GranuleId, outcome: ProcessingOutcome | None = None
    ) -> dict[ProcessingOutcome, list[GranuleProcessingEvent]]:
        """List events by outcome"""
        if outcome:
            outcomes = [outcome]
        else:
            outcomes = list(ProcessingOutcome)

        events = defaultdict(list)
        for outcome in outcomes:
            for path in self._list_logs_for_outcome(str(granule_id), outcome):
                event, outcome = self._path_to_event_outcome(path)
                events[outcome].append(event)

        return dict(events)

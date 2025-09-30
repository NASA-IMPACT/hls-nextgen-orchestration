"""Log granule processing event information"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Optional

from boto_session_manager import BotoSesManager
from botocore.exceptions import ClientError
from s3pathlib import S3Path

from common.aws_batch import (
    JobDetails,
)
from common.models import (
    GranuleId,
    GranuleProcessingEvent,
    ProcessingState,
)

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import (
        JobDetailTypeDef,
    )


class NoSuchEventAttemptExists(FileNotFoundError):
    """Raised if the logs for the GranuleProcessingEvent doesn't exist"""


@dataclass
class GranuleEventLog:
    """HLS processing job attempt details"""

    granule_id: str
    attempt: int
    state: ProcessingState
    job_info: Optional[JobDetailTypeDef] = None

    def to_json(self) -> str:
        """Export to JSON (enum dumped by name)"""
        return json.dumps(
            {
                "granule_id": self.granule_id,
                "attempt": self.attempt,
                "state": self.state.name,
                "job_info": self.job_info,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> GranuleEventLog:
        """Load from JSON"""
        data = json.loads(json_str)
        job_info = data.get("job_info")
        state = ProcessingState[data["state"]]
        return cls(
            granule_id=data["granule_id"],
            attempt=data["attempt"],
            state=state,
            job_info=job_info,
        )


@dataclass
class GranuleLoggerService:
    """Log granule processing details

    The granule logger describes staates from "granule processing events"
    by using S3 as a store for logs and state breadcrumbs. In order to
    predictably list or search for information about our granule processing
    system this organizes log information into a set of prefixes based on
    information:

    ```
    ./logs/state={state}/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/
    ```

    This organizes logs by state("success", "failure", "submitted", "awaiting")
    first as we anticipate wanting to search and analyze jobs that have "failed"
    more than the successes.

    Within each prefix job attempts are organized by the attempt. For example:

    ```
    ./logs/state=failure/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/attempt=1.json
    ./logs/state=success/acquisition_date={YYYY-MM-DD}/granule_id={GRANULE_ID}/attempt=2.json
    ```

    When a successful attempt has been logged, any previous attempts that had failures
    are removed from the failure prefix and reorganized into the "success" prefix to
    help prune the prefix containing failures.
    """

    bucket: str
    logs_prefix: str
    bsm: BotoSesManager = field(default_factory=BotoSesManager)

    # mapping of ProcessingState to S3 path component
    state_to_prefix: ClassVar[dict[ProcessingState, str]] = {
        ProcessingState.SUCCESS: "success",
        ProcessingState.FAILURE_NONRETRYABLE: "failure_nonretryable",
        ProcessingState.FAILURE_RETRYABLE: "failure_retryable",
        ProcessingState.AWAITING: "awaiting",
        ProcessingState.SUBMITTED: "submitted",
    }
    prefix_to_state: ClassVar[dict[str, ProcessingState]] = {
        value: key for key, value in state_to_prefix.items()
    }

    # regex to parse the log object into components
    log_path_regex: ClassVar[re.Pattern[str]] = re.compile(
        "/".join(
            [
                r"state=(?P<state>\w+)",
                r"platform=(?P<platform>\w+)",
                r"acquisition_date=(?P<acquisition_date>[\d-]+)",
                r"granule_id=(?P<granule_id>[\w\.]+)",
                r"attempt=(?P<attempt>[0-9]+)\.json$",
            ]
        )
    )
    # regex to match on attempt object name
    attempt_log_regex: ClassVar[re.Pattern[str]] = re.compile(r"^attempt=[0-9]+\.json$")

    def _prefix_for_granule_id_state(
        self, granule_id: GranuleId, state: ProcessingState
    ) -> S3Path:
        """Return the S3 path for storing this granule's info"""
        date = granule_id.begin_datetime.strftime("%Y-%m-%d")
        return S3Path(
            self.bucket,
            self.logs_prefix.rstrip("/"),
            f"state={self.state_to_prefix[state]}",
            f"platform={granule_id.platform}",
            f"acquisition_date={date}",
            f"granule_id={str(granule_id)}",
        )

    def _path_for_event_state(
        self,
        event: GranuleProcessingEvent,
        state: ProcessingState,
    ) -> S3Path:
        granule_id = GranuleId.from_str(event.granule_id)
        prefix = self._prefix_for_granule_id_state(granule_id, state)
        return S3Path(prefix, f"attempt={event.attempt}.json")

    def _path_to_event_state(
        self,
        log_artifact: S3Path,
    ) -> tuple[GranuleProcessingEvent, ProcessingState]:
        """Determine an event info from a log artifact path

        This is the inverse of the `_path_for_event_state`
        """
        path = log_artifact.key.removeprefix(self.logs_prefix).lstrip("/")
        match = self.log_path_regex.match(path)
        if not match:
            raise ValueError(
                f"Cannot parse {log_artifact.uri} into a GranuleProcessingEvent"
            )

        granule_id = match.group("granule_id")
        attempt = int(match.group("attempt"))
        state = self.prefix_to_state[match.group("state")]

        return (
            GranuleProcessingEvent(granule_id, attempt),
            state,
        )

    def _filter_attempt_log(self, path: S3Path) -> bool:
        return bool(self.attempt_log_regex.match(path.basename))

    def _list_logs_for_state(
        self, granule_id: str, state: ProcessingState
    ) -> list[S3Path]:
        """Helper function to find logs for some state"""
        prefix = self._prefix_for_granule_id_state(
            GranuleId.from_str(granule_id), state
        )
        paths = []
        for path in prefix.iter_objects(bsm=self.bsm).filter(self._filter_attempt_log):
            paths.append(path)
        return paths

    def _clean_previous_states(self, granule_id: str, state: ProcessingState) -> None:
        """Cleanup previous states"""
        for state_path in self._list_logs_for_state(granule_id, state):
            event, state = self._path_to_event_state(state_path)
            if state in (
                ProcessingState.FAILURE_NONRETRYABLE,
                ProcessingState.FAILURE_RETRYABLE,
                ProcessingState.SUBMITTED,
            ):
                success_path = self._path_for_event_state(
                    event, ProcessingState.SUCCESS
                )
                state_path.copy_to(success_path, bsm=self.bsm, overwrite=True)
            state_path.delete(bsm=self.bsm)

    def put_event(self, event: GranuleProcessingEvent, state: ProcessingState) -> None:
        s3path = self._path_for_event_state(event=event, state=state)
        event_log = GranuleEventLog(
            granule_id=event.granule_id,
            attempt=event.attempt,
            state=state,
        )

        s3path.write_text(event_log.to_json(), bsm=self.bsm)
        if state == ProcessingState.SUBMITTED:
            self._clean_previous_states(event.granule_id, ProcessingState.AWAITING)

    def put_event_details(self, details: JobDetails) -> None:
        """Log event details"""
        event = details.get_granule_event()
        job_state = details.get_job_state()
        s3path = self._path_for_event_state(event, job_state)
        event_log = GranuleEventLog(
            granule_id=event.granule_id,
            attempt=event.attempt,
            state=job_state,
            job_info=details.get_job_info(),
        )
        s3path.write_text(event_log.to_json(), bsm=self.bsm)
        if job_state in (
            ProcessingState.FAILURE_NONRETRYABLE,
            ProcessingState.FAILURE_RETRYABLE,
        ):
            self._clean_previous_states(event.granule_id, ProcessingState.SUBMITTED)
        if job_state == ProcessingState.SUCCESS:
            self._clean_previous_states(
                event.granule_id, ProcessingState.FAILURE_NONRETRYABLE
            )
            self._clean_previous_states(
                event.granule_id, ProcessingState.FAILURE_RETRYABLE
            )
            self._clean_previous_states(event.granule_id, ProcessingState.SUBMITTED)

    def get_event_details(self, event: GranuleProcessingEvent) -> JobDetails | None:
        """Get event details for an event

        Raises
        ------
        NoSuchEventAttemptExists
            Raised if the event provided doesn't exist in the logs
        """
        for state in ProcessingState:
            path = self._path_for_event_state(event, state)
            try:
                data = path.read_text(bsm=self.bsm)
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise
            else:
                event_log = GranuleEventLog.from_json(data)
                if event_log.job_info:
                    return JobDetails(event_log.job_info)
                else:
                    return None

        raise NoSuchEventAttemptExists(f"Cannot find logs for {event}")

    def list_events(
        self, granule_id: str | GranuleId, state: ProcessingState | None = None
    ) -> dict[ProcessingState, list[GranuleProcessingEvent]]:
        """List events by state"""
        if state:
            states = [state]
        else:
            states = list(ProcessingState)

        events = defaultdict(list)
        for state in states:
            for path in self._list_logs_for_state(str(granule_id), state):
                event, state = self._path_to_event_state(path)
                events[state].append(event)

        return dict(events)

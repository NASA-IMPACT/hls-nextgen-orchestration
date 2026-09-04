"""S3-backed three-object log store for granule processing state."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from common.models import ProcessingState

logger = logging.getLogger(__name__)


@dataclass
class ProcessingEventRecord:
    """A single state-transition event appended to a canonical record."""

    state: str
    ts: str  # ISO-8601 timestamp
    batch_job_id: str | None = None
    log_group_name: str | None = None
    log_stream_name: str | None = None
    exit_code: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class S3RecordStore:
    """Three-object S3 log schema.

    Objects written per granule attempt:

    1. Canonical record (append-only events array):
       records/{workflow}/{acquisition_date}/{source_granule_id}/{attempt:03d}.json

    2. State pointer (write-new / delete-old, minimal JSON body):
       state/{STATE}/{workflow}/{acquisition_date}/{source_granule_id}/{attempt:03d}

    3. Output index (empty body, terminal states only):
       outputs/{STATE}/{workflow}/{acquisition_date}/{output_granule_id}
    """

    bucket: str
    client: Any = field(default_factory=lambda: boto3.client("s3"))

    # ------------------------------------------------------------------ keys

    @staticmethod
    def canonical_key(
        workflow: str, acquisition_date: str, source_granule_id: str, attempt: int
    ) -> str:
        return (
            f"records/workflow={workflow}/acquisition_date={acquisition_date}"
            f"/source_granule_id={source_granule_id}/{attempt:03d}.json"
        )

    @staticmethod
    def _state_prefix(
        state: ProcessingState, workflow: str, acquisition_date: str
    ) -> str:
        return (
            f"state/state={state.name}/workflow={workflow}"
            f"/acquisition_date={acquisition_date}/"
        )

    @staticmethod
    def state_pointer_key(
        state: ProcessingState,
        workflow: str,
        acquisition_date: str,
        source_granule_id: str,
        attempt: int,
    ) -> str:
        return (
            S3RecordStore._state_prefix(state, workflow, acquisition_date)
            + f"source_granule_id={source_granule_id}/{attempt:03d}"
        )

    @staticmethod
    def output_index_key(
        state: ProcessingState,
        workflow: str,
        acquisition_date: str,
        output_granule_id: str,
    ) -> str:
        return (
            f"outputs/state={state.name}/workflow={workflow}"
            f"/acquisition_date={acquisition_date}/{output_granule_id}"
        )

    # -------------------------------------------------------- canonical record

    def append_canonical_event(
        self,
        *,
        source_granule_id: str,
        output_granule_id: str,
        workflow: str,
        acquisition_date: str,
        attempt: int,
        event: ProcessingEventRecord,
        batch_job_id: str | None = None,
        shadow: bool = False,
    ) -> None:
        """Append a state-transition event to the canonical record.

        Creates the record on first call; overwrites with the appended events
        array on subsequent calls. Not atomic — duplicate events on Lambda
        retries are acceptable for observability records.
        """
        key = self.canonical_key(workflow, acquisition_date, source_granule_id, attempt)
        record: dict[str, Any]
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            record = json.loads(resp["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchKey":
                raise
            record = {
                "source_granule_id": source_granule_id,
                "output_granule_id": output_granule_id,
                "workflow": workflow,
                "acquisition_date": acquisition_date,
                "attempt": attempt,
                "batch_job_id": batch_job_id,
                "shadow": shadow,
                "events": [],
                "current_state": event.state,
            }

        record["events"].append(event.to_dict())
        record["current_state"] = event.state

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(record).encode(),
            ContentType="application/json",
        )

    # ---------------------------------------------------------- state pointer

    def write_state_pointer(
        self,
        *,
        workflow: str,
        acquisition_date: str,
        source_granule_id: str,
        attempt: int,
        new_state: ProcessingState,
        old_state: ProcessingState | None,
        output_granule_id: str,
    ) -> None:
        """Write new state pointer and delete the old one."""
        body = json.dumps(
            {
                "source_granule_id": source_granule_id,
                "output_granule_id": output_granule_id,
                "attempt": attempt,
            }
        ).encode()
        new_key = self.state_pointer_key(
            new_state, workflow, acquisition_date, source_granule_id, attempt
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=new_key,
            Body=body,
            ContentType="application/json",
        )
        if old_state is not None:
            old_key = self.state_pointer_key(
                old_state, workflow, acquisition_date, source_granule_id, attempt
            )
            try:
                self.client.delete_object(Bucket=self.bucket, Key=old_key)
            except ClientError:
                logger.warning("Failed to delete old state pointer %s", old_key)

    def write_state_pointer_conditional(
        self,
        *,
        workflow: str,
        acquisition_date: str,
        source_granule_id: str,
        attempt: int,
        state: ProcessingState,
        output_granule_id: str,
    ) -> bool:
        """Write state pointer only if it does not already exist.

        Uses S3 conditional write (IfNoneMatch='*') to prevent duplicate
        submissions when ancillary-trigger fires multiple times for the same
        dated prefix.

        Returns True if the pointer was written, False if it already existed.
        """
        body = json.dumps(
            {
                "source_granule_id": source_granule_id,
                "output_granule_id": output_granule_id,
                "attempt": attempt,
            }
        ).encode()
        key = self.state_pointer_key(
            state, workflow, acquisition_date, source_granule_id, attempt
        )
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                IfNoneMatch="*",
            )
            return True
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ConditionalRequestConflict", "PreconditionFailed"):
                return False
            raise

    def delete_state_pointer(
        self,
        *,
        workflow: str,
        acquisition_date: str,
        source_granule_id: str,
        attempt: int,
        state: ProcessingState,
    ) -> None:
        """Delete a state pointer object."""
        key = self.state_pointer_key(
            state, workflow, acquisition_date, source_granule_id, attempt
        )
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError:
            logger.warning("Failed to delete state pointer %s", key)

    # ---------------------------------------------------------- output index

    def write_output_index(
        self,
        *,
        workflow: str,
        acquisition_date: str,
        output_granule_id: str,
        state: ProcessingState,
    ) -> None:
        """Write an empty output index entry for a terminal state."""
        key = self.output_index_key(
            state, workflow, acquisition_date, output_granule_id
        )
        self.client.put_object(Bucket=self.bucket, Key=key, Body=b"")

    # -------------------------------------------------- ancillary-trigger scan

    def list_awaiting(
        self, workflow: str, acquisition_date: str
    ) -> list[dict[str, Any]]:
        """List all AWAITING state pointers for a workflow/date.

        Returns a list of dicts parsed from the pointer JSON bodies,
        each containing source_granule_id, output_granule_id, attempt.
        """
        prefix = self._state_prefix(
            ProcessingState.AWAITING, workflow, acquisition_date
        )
        results: list[dict[str, Any]] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                try:
                    resp = self.client.get_object(Bucket=self.bucket, Key=obj["Key"])
                    data = json.loads(resp["Body"].read())
                    results.append(data)
                except (ClientError, json.JSONDecodeError):
                    logger.warning("Skipping unreadable pointer %s", obj["Key"])
        return results

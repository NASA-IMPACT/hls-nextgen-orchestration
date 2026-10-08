"""Job types, submitter states, and Batch submission for HLS processing jobs.

Shared by the submission Lambdas, the job monitor and resubmit Lambdas, and
the CDK stack, so each job type is declared once.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from batch_event_job_monitor import JobContext, JobGroup, S3RecordStore, submit_job
from batch_event_job_monitor.models import (
    ExitCodeOutcomes,
    ExitCodeOutcomesBuilder,
    JobTypeConfig,
    ProcessingState,
    RetryPolicy,
)

from common.models import (
    EXIT_CODE_CLOUDY,
    EXIT_CODE_LOW_SUN_ANGLE,
    GranuleProcessingEvent,
)

if TYPE_CHECKING:
    from mypy_boto3_batch import BatchClient

SENTINEL = "sentinel"
"""Job type of the Sentinel-2 jobs this system submits."""

PHASE0_SENTINEL = "phase0-sentinel"
PHASE0_LANDSAT_AC = "phase0-landsat-ac"
PHASE0_LANDSAT_TILE = "phase0-landsat-tile"
PHASE0_JOB_TYPES = (PHASE0_SENTINEL, PHASE0_LANDSAT_AC, PHASE0_LANDSAT_TILE)
"""Job types of the existing system's jobs, monitored in shadow mode."""

JOB_TYPES = (SENTINEL, *PHASE0_JOB_TYPES)

ACQUISITION_DATE = "acquisition_date"
"""The one partition field every job type is keyed by (YYYY-MM-DD)."""

AWAITING_ANCILLARY = ProcessingState.submitter("AWAITING_ANCILLARY")
"""A granule waiting on the ancillary data it needs to be submitted."""

SUBMITTED = ProcessingState.submitter("SUBMITTED")
"""A granule claimed for submission, written just before calling SubmitJob."""

SUBMITTER_STATES = (AWAITING_ANCILLARY, SUBMITTED)


def record_store() -> S3RecordStore:
    """The processing bucket's record store, from the Lambda environment."""
    return S3RecordStore(
        bucket=os.environ["PROCESSING_BUCKET_NAME"],
        key_prefix=os.environ.get("PROCESSING_KEY_PREFIX", ""),
    )


def exit_code_outcomes() -> ExitCodeOutcomes:
    """Exit codes the HLS containers use for expected, non-retryable outcomes."""
    return (
        ExitCodeOutcomesBuilder()
        .add(EXIT_CODE_LOW_SUN_ANGLE, "LOW_SUN_ANGLE", dlq=False)
        .add(EXIT_CODE_CLOUDY, "CLOUDY", dlq=False)
        .build()
    )


def sentinel_job_type_config(
    *, job_queue_arn: str, job_definition_arn: str, max_attempts: int
) -> JobTypeConfig:
    """Monitoring config for the Sentinel-2 jobs this system submits."""
    return JobTypeConfig(
        job_queue_arn=job_queue_arn,
        job_definition_arn=job_definition_arn,
        retry_policy=RetryPolicy(max_attempts=max_attempts),
        exit_code_outcomes=exit_code_outcomes(),
        submitter_states=tuple(state.name for state in SUBMITTER_STATES),
    )


def phase0_job_type_config(
    *, job_queue_arn: str, job_definition_arn: str
) -> JobTypeConfig:
    """Monitoring config for one of the existing system's job types.

    Its jobs carry no bejm_* parameters, and their failures are retried by
    the existing system, so they are recorded but never routed. Only their
    outcome is tracked.
    """
    return JobTypeConfig(
        job_queue_arn=job_queue_arn,
        job_definition_arn=job_definition_arn,
        exit_code_outcomes=exit_code_outcomes(),
        requires_bejm_parameters=False,
        route_failures=False,
        tracked_statuses=("SUCCEEDED", "FAILED"),
    )


def sentinel_job_group(
    *,
    acquisition_date: str,
    source_granule_ids: list[str],
    output_granule_id: str,
    attempt: int = 1,
) -> JobGroup:
    """The JobGroup for one Sentinel-2 output granule."""
    return JobGroup(
        job_type=SENTINEL,
        partition_fields={ACQUISITION_DATE: acquisition_date},
        input_entity_ids=source_granule_ids,
        output_entity_id=output_granule_id,
        attempt=attempt,
    )


def build_submit_job_params(
    job_group: JobGroup,
    *,
    job_queue: str,
    job_definition: str,
    output_bucket: str,
) -> dict[str, Any]:
    """SubmitJob kwargs for a job group, setting the container's environment.

    The bejm_* parameters are added by submit_job/resubmit_job.
    """
    event = GranuleProcessingEvent(
        workflow=job_group.job_type,
        acquisition_date=job_group.partition_fields[ACQUISITION_DATE],
        source_granule_ids=list(job_group.input_entity_ids),
        output_granule_id=job_group.output_entity_id,
        attempt=job_group.attempt,
    )
    return {
        "jobName": job_group.batch_job_name(),
        "jobQueue": job_queue,
        "jobDefinition": job_definition,
        "containerOverrides": {
            "environment": [
                {"name": "OUTPUT_BUCKET", "value": output_bucket},
                *event.to_environment(),
            ]
        },
    }


def write_awaiting_ancillary(store: S3RecordStore, job_group: JobGroup) -> None:
    """Record each of a job group's granules as waiting on ancillary data."""
    for context in job_group.contexts():
        store.write_state_pointer_conditional(context=context, state=AWAITING_ANCILLARY)


def claim_and_submit(
    *,
    store: S3RecordStore,
    batch_client: BatchClient,
    job_group: JobGroup,
    job_queue: str,
    job_definition: str,
    output_bucket: str,
) -> str | None:
    """Submit a job group unless another invocation already claimed it.

    Claims every granule with a SUBMITTED pointer before calling SubmitJob,
    so a duplicate trigger cannot submit the same granule twice. A failed
    claim or SubmitJob call releases the claims this call made, leaving the
    granules for a later attempt.

    Returns
    -------
    str or None
        The Batch job id, or None if another invocation holds a claim.
    """
    claimed: list[JobContext] = []
    for context in job_group.contexts():
        if not store.write_state_pointer_conditional(context=context, state=SUBMITTED):
            _release(store, claimed)
            return None
        claimed.append(context)

    try:
        job_id = submit_job(
            batch_client=batch_client,
            build_submit_job_params=lambda group: build_submit_job_params(
                group,
                job_queue=job_queue,
                job_definition=job_definition,
                output_bucket=output_bucket,
            ),
            job_group=job_group,
        )
    except Exception:
        _release(store, claimed)
        raise

    for context in claimed:
        store.delete_state_pointer(context=context, state=AWAITING_ANCILLARY)
    return job_id


def _release(store: S3RecordStore, contexts: list[JobContext]) -> None:
    for context in contexts:
        store.delete_state_pointer(context=context, state=SUBMITTED)

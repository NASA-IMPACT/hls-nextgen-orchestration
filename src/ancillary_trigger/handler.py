"""Ancillary-trigger Lambda — fan-out.

Triggered when new ancillary data lands on S3 (see common.ancillary). Lists the
AWAITING_ANCILLARY state pointers for the data's acquisition date and enqueues
one SQS message per output granule (so twin granules stay one job) to the
internal ancillary-submit queue.

Fan-out is fast (list + SQS send_message_batch), so timeout risk is negligible
regardless of how many granules are waiting for a given date.

Idempotency is handled downstream in the ancillary-submit Lambda, which claims
each granule with a conditional SUBMITTED state pointer before submitting.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import boto3

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common.ancillary import ancillary_source_from_env
from common.jobs import ACQUISITION_DATE, AWAITING_ANCILLARY, SENTINEL, record_store

logger = Logger()
tracer = Tracer()


@tracer.capture_method
def process_aux_event(
    *,
    s3_key: str,
    submit_queue_url: str,
) -> int:
    """List granules awaiting *s3_key*'s date and enqueue one message per output.

    Returns the number of messages enqueued.
    """
    date = ancillary_source_from_env().acquisition_date(s3_key)
    if date is None:
        logger.info("Key %s is not a recognised ancillary file; skipping", s3_key)
        return 0
    acquisition_date = date.isoformat()

    store = record_store()
    sqs = boto3.client("sqs")

    pointers = store.list_by_state(
        job_type=SENTINEL,
        state=AWAITING_ANCILLARY,
        partition_fields={ACQUISITION_DATE: acquisition_date},
    )
    # One message per output granule and attempt, covering all its sources
    groups: dict[tuple[str, int], list[str]] = defaultdict(list)
    for pointer in pointers:
        key = (pointer["output_entity_id"], pointer["attempt"])
        groups[key].append(pointer["input_entity_id"])
    logger.info(
        "Found %d granules awaiting ancillary data for date=%s; enqueuing",
        len(groups),
        acquisition_date,
    )

    if not groups:
        return 0

    entries: list[SendMessageBatchRequestEntryTypeDef] = [
        {
            "Id": str(i),
            "MessageBody": json.dumps(
                {
                    "acquisition_date": acquisition_date,
                    "source_granule_ids": sorted(source_granule_ids),
                    "output_granule_id": output_granule_id,
                    "attempt": attempt,
                }
            ),
        }
        for i, ((output_granule_id, attempt), source_granule_ids) in enumerate(
            sorted(groups.items())
        )
    ]
    # SQS send_message_batch accepts at most 10 messages per call
    for batch_start in range(0, len(entries), 10):
        sqs.send_message_batch(
            QueueUrl=submit_queue_url,
            Entries=entries[batch_start : batch_start + 10],
        )

    return len(entries)


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, int]:
    """Lambda entry point for ancillary S3 PutObject events (SNS → SQS)."""
    submit_queue_url = os.environ["ANCILLARY_SUBMIT_QUEUE_URL"]

    total_enqueued = 0
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        # SNS-wrapped S3 event
        if "Message" in body:
            s3_event = json.loads(body["Message"])
            s3_records = s3_event.get("Records", [])
        else:
            s3_records = body.get("Records", [])

        for s3_record in s3_records:
            s3_key = s3_record["s3"]["object"]["key"]
            total_enqueued += process_aux_event(
                s3_key=s3_key,
                submit_queue_url=submit_queue_url,
            )

    return {"enqueued": total_enqueued}

"""Ancillary-trigger Lambda — fan-out.

Triggered when new ancillary (LaSRC LADS) data lands on S3. Scans
``state/AWAITING/sentinel/{acquisition_date}/`` for granules that are now
ready to process and enqueues one SQS message per granule to the internal
ancillary-submit queue.

Fan-out is fast (list + SQS send_message_batch), so timeout risk is negligible
regardless of how many AWAITING granules there are for a given date.

Idempotency is handled downstream in the ancillary-submit Lambda via a
conditional S3 PutObject (IfNoneMatch='*') on the SUBMITTED state pointer.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import TYPE_CHECKING, Any

import boto3

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from common import S3RecordStore

logger = Logger()
tracer = Tracer()

_WORKFLOW = "sentinel"

# Matches: lasrc_aux/LADS/YYYY/VJ104ANC.AYYYYDDD  or VNP04ANC.AYYYYDDD
_AUX_KEY_RE = re.compile(r"lasrc_aux/LADS/(\d{4})/(?:VJ104ANC|VNP04ANC)\.A(\d{7})")


def _parse_aux_key(s3_key: str) -> str | None:
    """Extract YYYY-MM-DD acquisition date from an ancillary S3 key.

    Returns None if the key does not match the expected pattern.
    """
    m = _AUX_KEY_RE.search(s3_key)
    if not m:
        return None
    ydoy = m.group(2)  # YYYYDDD
    try:
        acq_dt = dt.datetime.strptime(ydoy, "%Y%j")
        return acq_dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


@tracer.capture_method
def process_aux_event(
    *,
    s3_key: str,
    processing_bucket: str,
    submit_queue_url: str,
) -> int:
    """List AWAITING granules for *s3_key*'s date and enqueue one message each.

    Returns the number of messages enqueued.
    """
    acquisition_date = _parse_aux_key(s3_key)
    if acquisition_date is None:
        logger.info("Key %s is not a recognised ancillary file; skipping", s3_key)
        return 0

    store = S3RecordStore(bucket=processing_bucket)
    sqs = boto3.client("sqs")

    awaiting = store.list_awaiting(_WORKFLOW, acquisition_date)
    logger.info(
        "Found %d AWAITING granules for date=%s; enqueuing to submit queue",
        len(awaiting),
        acquisition_date,
    )

    if not awaiting:
        return 0

    entries: list[SendMessageBatchRequestEntryTypeDef] = [
        {
            "Id": str(i),
            "MessageBody": json.dumps({**ptr, "acquisition_date": acquisition_date}),
        }
        for i, ptr in enumerate(awaiting)
    ]
    # SQS send_message_batch accepts at most 10 messages per call
    for batch_start in range(0, len(entries), 10):
        sqs.send_message_batch(
            QueueUrl=submit_queue_url,
            Entries=entries[batch_start : batch_start + 10],
        )

    return len(awaiting)


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, int]:
    """Lambda entry point for ancillary S3 PutObject events (SNS → SQS)."""
    processing_bucket = os.environ["PROCESSING_BUCKET_NAME"]
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
                processing_bucket=processing_bucket,
                submit_queue_url=submit_queue_url,
            )

    return {"enqueued": total_enqueued}

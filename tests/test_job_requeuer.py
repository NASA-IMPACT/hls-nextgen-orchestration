"""Tests for `requeuer` Lambda"""

import json
from unittest.mock import MagicMock

from aws_lambda_typing.events import SQSEvent

from job_requeuer.handler import job_requeuer


def test_requeuer_many(
    bucket: str,
    mocked_batch_client_submit_job: MagicMock,
) -> None:
    """Test resubmitting several jobs from SQS queue"""
    granule_attempts = {
        "HLS.S30.T01GBH.2023051T214901.v2.0": 0,
        "HLS.S30.T09ASD.2023123T214901.v2.0": 1,
        "HLS.S30.T42WOW.2023321T214901.v2.0": 2,
    }
    job_ids = job_requeuer(
        job_queue="queue",
        job_definition_name="job-def",
        output_bucket=bucket,
        event=SQSEvent(
            Records=[
                {"body": json.dumps({"granule_id": granule_id, "attempt": attempt})}
                for granule_id, attempt in granule_attempts.items()
            ]
        ),
    )
    assert len(job_ids) == len(granule_attempts)
    for call in mocked_batch_client_submit_job.mock_calls:
        assert call.kwargs["output_bucket"] == bucket
        event = call.kwargs["event"]
        assert event.attempt == granule_attempts[event.granule_id] + 1

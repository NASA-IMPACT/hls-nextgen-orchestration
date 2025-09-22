"""Tests for `common.granule_logger`"""

import pytest
from mypy_boto3_batch.type_defs import JobDetailTypeDef

from common import GranuleId, GranuleProcessingEvent, ProcessingOutcome
from common.aws_batch import JobDetails
from common.granule_logger import GranuleLoggerService


class TestGranuleLoggerService:
    """Test GranuleLoggerService"""

    @pytest.fixture
    def service(self, bucket: str) -> GranuleLoggerService:
        """Return an instance of the service with S3 bucket mocked with moto"""
        return GranuleLoggerService(bucket=bucket, logs_prefix="logs/")

    def test_prefix_to_outcomes_unique(self) -> None:
        """Sanity check each ProcessingOutcome has a unique prefix"""
        assert len(GranuleLoggerService.outcome_to_prefix) == len(
            GranuleLoggerService.prefix_to_outcome
        )

    def test_attempt_log_regex(self) -> None:
        """Sanity check log name regex"""
        assert GranuleLoggerService.attempt_log_regex.match("attempt=0.json")
        assert GranuleLoggerService.attempt_log_regex.match("attempt=10.json")
        assert not GranuleLoggerService.attempt_log_regex.match("attempt=json")
        assert not GranuleLoggerService.attempt_log_regex.match("attempt=42.log")

    @pytest.mark.parametrize("outcome", list(ProcessingOutcome))
    def test_path_for_event_outcome(
        self,
        service: GranuleLoggerService,
        granule_id: GranuleId,
        outcome: ProcessingOutcome,
    ) -> None:
        """Test correctly construct and infer S3Path for an event/outcome"""
        event = GranuleProcessingEvent(
            granule_id=str(granule_id),
            attempt=1,
        )

        path = service._path_for_event_outcome(event, outcome)
        test_event, test_outcome = service._path_to_event_outcome(path)
        assert test_event == event
        assert test_outcome == outcome

    def test_log_failure_and_success(
        self,
        service: GranuleLoggerService,
        granule_id: GranuleId,
        job_detail_failed_spot: JobDetailTypeDef,
    ) -> None:
        """Test we can log a sequence of failures and then a final success"""
        # First failure
        batch_details = job_detail_failed_spot.copy()
        first_event = GranuleProcessingEvent(str(granule_id), 0)
        batch_details["container"]["environment"] = first_event.to_environment()
        batch_details["container"].pop("exitCode", None)  # spot failure
        details = JobDetails(batch_details)

        service.put_event_details(details)
        restored_details = service.get_event_details(first_event)
        assert restored_details == details

        # Second failure
        batch_details = job_detail_failed_spot.copy()
        second_event = first_event.new_attempt()
        batch_details["container"]["environment"] = second_event.to_environment()
        batch_details["container"]["exitCode"] = 1  # some kind of bug
        details = JobDetails(batch_details)

        service.put_event_details(details)
        restored_details = service.get_event_details(second_event)
        assert restored_details == details

        # Two failures should exist
        list_events = service.list_events(granule_id)
        assert set(list_events[ProcessingOutcome.FAILURE]) == {
            first_event,
            second_event,
        }

        # We fixed a bug and it succeeds
        batch_details = job_detail_failed_spot.copy()
        third_event = second_event.new_attempt()
        batch_details["container"]["environment"] = third_event.to_environment()
        batch_details["container"]["exitCode"] = 0
        details = JobDetails(batch_details)

        service.put_event_details(details)
        restored_details = service.get_event_details(third_event)
        assert restored_details == details

        # All logs have been moved to "success" since the job is done
        list_events = service.list_events(granule_id)
        assert ProcessingOutcome.FAILURE not in list_events
        assert set(list_events[ProcessingOutcome.SUCCESS]) == {
            first_event,
            second_event,
            third_event,
        }

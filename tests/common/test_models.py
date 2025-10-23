import pytest

from common.models import (
    GranuleId,
    GranuleProcessingEvent,
    ProcessingState,
)

# def test_job_outcome_covers_processing_outcome() -> None:
# """Ensure our JobOutcome.processing_outcome covers all ProcessingOutcomes"""
# processing_outcomes = set(ProcessingOutcome)
# job_processing_outcomes = {outcome.processing_outcome for outcome in JobOutcome}
# assert processing_outcomes == job_processing_outcomes


class TestProcessingState:
    """Sanity checks for enum properties"""

    def test_previous_states(self) -> None:
        """Ensure enum has exhaustive match for previous states"""
        for state in list(ProcessingState):
            previous_states = state.previous_states()
            assert isinstance(previous_states, tuple)
            assert all(
                isinstance(previous_state, ProcessingState)
                for previous_state in previous_states
            )

    def test_migrate_logs_to_state(self) -> None:
        """Ensure enum has exhaustive match for previous states"""
        for state in list(ProcessingState):
            migrate_state = state.migrate_logs_to_state()
            assert migrate_state is None or isinstance(migrate_state, ProcessingState)


class TestGranuleId:
    """Tests for GranuleId"""

    @pytest.mark.parametrize(
        "granule_id",
        [
            "HLS.S30.T01GBH.2023051T214901.v2.0",
            "HLS.L30.T18VUJ.2024321T161235.v2.0",
        ],
    )
    def test_to_from_granule_id(self, granule_id: str) -> None:
        """Test to/from string"""
        granule_id_ = GranuleId.from_str(granule_id)
        test_granule_id = str(granule_id_)
        assert granule_id == test_granule_id


class TestGranuleProcessingEvent:
    """Test GranuleProcessingEvent"""

    @pytest.mark.parametrize("debug_bucket", ["foo", None])
    def test_to_from_envvar(self, debug_bucket: str | None) -> None:
        event = GranuleProcessingEvent(
            granule_id="foo",
            attempt=42,
            debug_bucket=debug_bucket,
        )
        env = event.to_envvar()
        event_from_envvar = GranuleProcessingEvent.from_envvar(env)

        assert event == event_from_envvar

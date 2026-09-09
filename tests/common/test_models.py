import pytest

from common.models import (
    EXIT_CODE_CLOUDY,
    EXIT_CODE_LOW_SUN_ANGLE,
    GranuleId,
    GranuleProcessingEvent,
    ProcessingState,
)


class TestProcessingState:
    def test_all_states_present(self) -> None:
        names = {s.name for s in ProcessingState}
        assert "CLOUDY" in names
        assert "LOW_SUN_ANGLE" in names
        assert "SUCCESS" in names
        assert "AWAITING" in names
        assert "SUBMITTED" in names
        assert "FAILURE_RETRYABLE" in names
        assert "FAILURE_NONRETRYABLE" in names

    def test_terminal_states(self) -> None:
        assert ProcessingState.SUCCESS.is_terminal()
        assert ProcessingState.CLOUDY.is_terminal()
        assert ProcessingState.LOW_SUN_ANGLE.is_terminal()
        assert ProcessingState.FAILURE_NONRETRYABLE.is_terminal()
        assert not ProcessingState.AWAITING.is_terminal()
        assert not ProcessingState.SUBMITTED.is_terminal()
        assert not ProcessingState.FAILURE_RETRYABLE.is_terminal()

    def test_exit_code_constants(self) -> None:
        assert EXIT_CODE_CLOUDY == 4
        assert EXIT_CODE_LOW_SUN_ANGLE == 3


class TestGranuleId:
    @pytest.mark.parametrize(
        "granule_id",
        [
            "HLS.S30.T01GBH.2023051T214901.v2.0",
            "HLS.L30.T18VUJ.2024321T161235.v2.0",
        ],
    )
    def test_to_from_granule_id(self, granule_id: str) -> None:
        granule_id_ = GranuleId.from_str(granule_id)
        assert str(granule_id_) == granule_id


class TestGranuleProcessingEvent:
    @pytest.mark.parametrize("debug_bucket", ["my-debug-bucket", None])
    def test_to_from_envvar(self, debug_bucket: str | None) -> None:
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date="2024-01-15",
            source_granule_ids=["S2A_MSIL1C_20240115T..._T18TYN_20240115T..."],
            output_granule_id="HLS.S30.T18TYN.2024015T154921.v2.0",
            attempt=2,
            debug_bucket=debug_bucket,
        )
        env = event.to_envvar()
        assert env["WORKFLOW"] == "sentinel"
        assert env["ACQUISITION_DATE"] == "2024-01-15"
        assert env["ATTEMPT"] == "2"

        recovered = GranuleProcessingEvent.from_envvar(env)
        assert recovered == event

    def test_source_granule_ids_comma_separated(self) -> None:
        ids = ["S2A_one", "S2A_two"]
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date="2024-01-15",
            source_granule_ids=ids,
            output_granule_id="HLS.S30.T18TYN.2024015T154921.v2.0",
        )
        env = event.to_envvar()
        assert env["SOURCE_GRANULE_IDS"] == "S2A_one,S2A_two"
        recovered = GranuleProcessingEvent.from_envvar(env)
        assert recovered.source_granule_ids == ids

    def test_to_from_json(self) -> None:
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date="2024-01-15",
            source_granule_ids=["S2A_foo"],
            output_granule_id="HLS.S30.T18TYN.2024015T154921.v2.0",
            attempt=1,
        )
        assert GranuleProcessingEvent.from_json(event.to_json()) == event

    def test_new_attempt(self) -> None:
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date="2024-01-15",
            source_granule_ids=["S2A_foo"],
            output_granule_id="HLS.S30.T18TYN.2024015T154921.v2.0",
            attempt=1,
        )
        next_event = event.new_attempt()
        assert next_event.attempt == 2
        assert next_event.workflow == event.workflow
        assert next_event.source_granule_ids == event.source_granule_ids

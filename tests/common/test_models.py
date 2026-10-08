import pytest

from common.models import GranuleId, GranuleProcessingEvent, convert_safe_id_to_hls_id
from tests.conftest import GRANULE_ID_STR, SAFE_ID


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


class TestConvertSafeIdToHlsId:
    def test_known_conversion(self) -> None:
        assert convert_safe_id_to_hls_id(SAFE_ID) == GRANULE_ID_STR

    @pytest.mark.parametrize(
        ["safe_id", "expected_tile"],
        [
            ("S2A_MSIL1C_20230817T154921_N0509_R011_T18TYN_20230817T204510", "T18TYN"),
            ("S2B_MSIL1C_20240115T160901_N0509_R097_T10SEG_20240115T180000", "T10SEG"),
        ],
    )
    def test_tile_extraction(self, safe_id: str, expected_tile: str) -> None:
        assert f".{expected_tile}." in convert_safe_id_to_hls_id(safe_id)


class TestGranuleProcessingEvent:
    @pytest.mark.parametrize("debug_bucket", ["my-debug-bucket", None])
    def test_to_envvar(self, debug_bucket: str | None) -> None:
        event = GranuleProcessingEvent(
            workflow="sentinel",
            acquisition_date="2024-01-15",
            source_granule_ids=["S2A_one", "S2A_two"],
            output_granule_id="HLS.S30.T18TYN.2024015T154921.v2.0",
            attempt=2,
            debug_bucket=debug_bucket,
        )
        env = event.to_envvar()
        assert env["WORKFLOW"] == "sentinel"
        assert env["ACQUISITION_DATE"] == "2024-01-15"
        assert env["SOURCE_GRANULE_IDS"] == "S2A_one,S2A_two"
        assert env["OUTPUT_GRANULE_ID"] == "HLS.S30.T18TYN.2024015T154921.v2.0"
        assert env["ATTEMPT"] == "2"
        assert env.get("DEBUG_BUCKET") == debug_bucket

    def test_to_environment(self) -> None:
        event = GranuleProcessingEvent(workflow="sentinel", acquisition_date="d")
        assert {"name": "WORKFLOW", "value": "sentinel"} in event.to_environment()

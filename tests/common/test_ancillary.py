"""Tests for ancillary data sources."""

import datetime as dt

import pytest
from mypy_boto3_s3 import S3Client

from common.ancillary import LadsSource, ancillary_source_from_env
from common.models import GranuleId


class TestLadsSource:
    @pytest.mark.parametrize(
        "key",
        [
            "lasrc_aux/LADS/2023/VJ104ANC.A2023229.002.2023230123456.hdf",
            "lasrc_aux/LADS/2023/VNP04ANC.A2023229",
        ],
    )
    def test_acquisition_date_of_a_lads_file(self, key: str) -> None:
        assert LadsSource("aux").acquisition_date(key) == dt.date(2023, 8, 17)

    @pytest.mark.parametrize(
        "key",
        ["some/other/file.tif", "lasrc_aux/LADS/2023/VJ104ANC.A2023999"],
    )
    def test_other_files_have_no_acquisition_date(self, key: str) -> None:
        assert LadsSource("aux").acquisition_date(key) is None

    def test_available_when_a_lads_file_exists(
        self, granule_id: GranuleId, aux_bucket: str, s3: S3Client
    ) -> None:
        date = granule_id.begin_datetime.date()
        assert LadsSource(aux_bucket).is_available(date, s3)

    def test_unavailable_when_no_lads_file_exists(self, s3: S3Client) -> None:
        s3.create_bucket(
            Bucket="empty-aux",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        assert not LadsSource("empty-aux").is_available(dt.date(2023, 8, 17), s3)


class TestAncillarySourceFromEnv:
    def test_defaults_to_lads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ANCILLARY_SOURCE", raising=False)
        monkeypatch.setenv("AUX_DATA_BUCKET_NAME", "aux")
        assert ancillary_source_from_env() == LadsSource("aux")

    def test_rejects_an_unknown_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANCILLARY_SOURCE", "bogus")
        monkeypatch.setenv("AUX_DATA_BUCKET_NAME", "aux")
        with pytest.raises(ValueError, match="bogus"):
            ancillary_source_from_env()

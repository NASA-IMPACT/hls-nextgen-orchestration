"""CDK construct for the Athena state-inventory database.

Creates a Glue database + S3-inventory table over the ``state/`` prefix of the
processing bucket, plus a view that parses each key into structured columns.

Key schema:
  ``state/state={STATE}/workflow={workflow}/acquisition_date={acquisition_date}/source_granule_id={source_granule_id}/{attempt:03d}``

The inventory snapshot (daily Parquet) is cheap to query and supports
reconciliation queries such as:

  - Count granules by state / workflow / acquisition_date
  - Find granules stuck in SUBMITTED or AWAITING for a given date
  - Diff today vs yesterday to measure throughput
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from aws_cdk import aws_glue as glue
from constructs import Construct

from .athena_common import (
    DT_PARTITION_KEY,
    create_inventory_table,
    create_presto_view,
)

# ruff: disable[E501]
_VIEW_COLUMNS = [
    glue.CfnTable.ColumnProperty(
        name="state",
        type="string",
        comment="Processing state (AWAITING, SUBMITTED, SUCCESS, CLOUDY, LOW_SUN_ANGLE, FAILURE_RETRYABLE, FAILURE_NONRETRYABLE).",
    ),
    # ruff: enable[E501]
    glue.CfnTable.ColumnProperty(
        name="workflow",
        type="string",
        comment="Processing workflow (sentinel, landsat-ac, landsat-tile).",
    ),
    glue.CfnTable.ColumnProperty(
        name="acquisition_date",
        type="string",
        comment="Granule acquisition date (YYYY-MM-DD).",
    ),
    glue.CfnTable.ColumnProperty(
        name="source_granule_id",
        type="string",
        comment="Source granule identifier (SAFE ID or scene ID).",
    ),
    glue.CfnTable.ColumnProperty(
        name="attempt",
        type="int",
        comment="Attempt number (0-indexed).",
    ),
    glue.CfnTable.ColumnProperty(
        name="last_modified_date",
        type="timestamp",
        comment="When the state pointer was last written.",
    ),
    glue.CfnTable.ColumnProperty(
        name="key",
        type="string",
        comment="Full S3 key of the state pointer object.",
    ),
]


class AthenaStateDatabase(Construct):
    """Athena database for reconciliation queries over state/ pointer objects."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        inventory_location_s3path: str,
        table_datetime_start: dt.datetime,
        table_name: str,
        view_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = database

        self.inventory_table = create_inventory_table(
            self,
            "InventoryTable",
            database=database,
            table_name=table_name,
            location=inventory_location_s3path,
            datetime_start=table_datetime_start,
        )

        self.state_view = create_presto_view(
            self,
            "StateView",
            database=database,
            database_name=database_name,
            view_name=view_name,
            sql=self._view_sql(table_name),
            columns=_VIEW_COLUMNS,
            partition_keys=[DT_PARTITION_KEY],
            depends_on=self.inventory_table,
        )

    @staticmethod
    def _view_sql(table_name: str) -> str:
        # ruff: disable[E501]
        return f"""
        SELECT
            regexp_extract(key, '/state=([^/]+)/',             1) AS state,
            regexp_extract(key, '/workflow=([^/]+)/',           1) AS workflow,
            regexp_extract(key, '/acquisition_date=([^/]+)/',   1) AS acquisition_date,
            regexp_extract(key, '/source_granule_id=([^/]+)/',  1) AS source_granule_id,
            CAST(regexp_extract(key, '/([0-9]{{3}})$', 1) AS INT) AS attempt,
            last_modified_date,
            key
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
          AND is_latest
          AND NOT is_delete_marker
        """
        # ruff: enable[E501]

"""CDK construct for the Athena output-index database.

Creates a Glue database + S3-inventory table over the ``outputs/`` prefix of the
processing bucket, plus a view that parses each key into structured columns.

Key schema:
  ``outputs/state={STATE}/workflow={workflow}/acquisition_date={acquisition_date}/{output_granule_id}``

Output-index entries are written by ``job_monitor`` at terminal state for all
outcomes (SUCCESS, CLOUDY, LOW_SUN_ANGLE, ...), keyed by ``output_granule_id``.
This makes the inventory the source for downstream reconciliation queries:

  - LP DAAC reconciliation: compare ``SUCCESS`` output granule IDs to the catalog
  - Coverage by date: count produced products by workflow / acquisition_date
  - Screen-out rates: CLOUDY / LOW_SUN_ANGLE counts over time
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
        comment="Terminal state (SUCCESS, CLOUDY, LOW_SUN_ANGLE, FAILURE_NONRETRYABLE).",
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
        name="output_granule_id",
        type="string",
        comment="HLS output product identifier.",
    ),
    glue.CfnTable.ColumnProperty(
        name="last_modified_date",
        type="timestamp",
        comment="When the output-index entry was written.",
    ),
    glue.CfnTable.ColumnProperty(
        name="key",
        type="string",
        comment="Full S3 key of the output-index object.",
    ),
]


class AthenaOutputsDatabase(Construct):
    """Athena database for reconciliation queries over outputs/ index objects."""

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

        self.outputs_view = create_presto_view(
            self,
            "OutputsView",
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
            regexp_extract(key, '/state=([^/]+)/',                  1) AS state,
            regexp_extract(key, '/workflow=([^/]+)/',               1) AS workflow,
            regexp_extract(key, '/acquisition_date=([^/]+)/',       1) AS acquisition_date,
            regexp_extract(key, '/acquisition_date=[^/]+/([^/]+)$', 1) AS output_granule_id,
            last_modified_date,
            key
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
          AND is_latest
          AND NOT is_delete_marker
        """
        # ruff: enable[E501]

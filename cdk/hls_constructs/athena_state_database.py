"""CDK construct for the Athena state-inventory database.

Creates a Glue database + S3-inventory table over the ``state/`` prefix of the
processing bucket, plus a view that parses each key into structured columns.

Key schema:
  ``state/{STATE}/{workflow}/{acquisition_date}/{source_granule_id}/{attempt:03d}``

The inventory snapshot (daily Parquet) is cheap to query and supports
reconciliation queries such as:

  - Count granules by state / workflow / acquisition_date
  - Find granules stuck in SUBMITTED or AWAITING for a given date
  - Diff today vs yesterday to measure throughput
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Any

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct


def _athena_to_presto(athena_type: str | None) -> str:
    if athena_type is None:
        raise ValueError("Cannot convert null Athena type")
    return {
        "string": "varchar",
        "struct": "row",
        "float": "real",
        "binary": "varbinary",
    }.get(athena_type.lower(), athena_type.lower())


# S3 inventory Parquet SerDe (SymlinkTextInputFormat + ParquetHiveSerDe)
_SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
_HIVE_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
_PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

_INVENTORY_COLUMNS = [
    glue.CfnTable.ColumnProperty(
        name="bucket",
        type="string",
        comment="Source bucket name.",
    ),
    glue.CfnTable.ColumnProperty(
        name="key",
        type="string",
        comment="Object key.",
    ),
    glue.CfnTable.ColumnProperty(
        name="version_id",
        type="string",
        comment="Object version ID.",
    ),
    glue.CfnTable.ColumnProperty(
        name="is_latest",
        type="boolean",
        comment="True if this is the current version.",
    ),
    glue.CfnTable.ColumnProperty(
        name="is_delete_marker",
        type="boolean",
        comment="True if this is a delete marker.",
    ),
    glue.CfnTable.ColumnProperty(
        name="last_modified_date",
        type="timestamp",
        comment="Object creation or last-modified date.",
    ),
]

_VIEW_COLUMNS = [
    glue.CfnTable.ColumnProperty(
        name="state",
        type="string",
        comment="Processing state (AWAITING, SUBMITTED, SUCCESS, …).",
    ),
    glue.CfnTable.ColumnProperty(
        name="workflow",
        type="string",
        comment="Processing workflow (sentinel, landsat-ac, …).",
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
        database_name: str,
        inventory_location_s3path: str,
        table_datetime_start: dt.datetime,
        table_name: str,
        view_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = glue.CfnDatabase(
            self,
            "Database",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description=(
                    "Athena database for HLS NextGen state-pointer reconciliation."
                ),
            ),
        )

        self.inventory_table = self._create_inventory_table(
            table_name=table_name,
            inventory_location_s3path=inventory_location_s3path,
            table_datetime_start=table_datetime_start,
        )

        self.state_view = self._create_state_view(
            view_name=view_name,
            table_name=table_name,
        )

    def _create_inventory_table(
        self,
        *,
        table_name: str,
        inventory_location_s3path: str,
        table_datetime_start: dt.datetime,
    ) -> glue.CfnTable:
        table = glue.CfnTable(
            self,
            "InventoryTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=self.database.database_name,  # type: ignore[arg-type]
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters={
                    "EXTERNAL": "TRUE",
                    "projection.enabled": "true",
                    "projection.dt.type": "date",
                    "projection.dt.format": "yyyy-MM-dd-HH-mm",
                    "projection.dt.range": (
                        f"{table_datetime_start:%Y-%m-%d-%H-%M},NOW"
                    ),
                    "projection.dt.interval": "1",
                    "projection.dt.interval.unit": "HOURS",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        type="string",
                        comment="Inventory report datetime (yyyy-MM-dd-HH-mm).",
                    )
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=_INVENTORY_COLUMNS,
                    location=inventory_location_s3path,
                    input_format=_SYMLINK_INPUT_FORMAT,
                    output_format=_HIVE_OUTPUT_FORMAT,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=_PARQUET_SERDE,
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_dependency(self.database)
        return table

    def _create_state_view(self, *, view_name: str, table_name: str) -> glue.CfnTable:
        # ruff: disable[E501]
        sql = f"""
        SELECT
            regexp_extract(key, '^state/([^/]+)/', 1)                   AS state,
            regexp_extract(key, '^state/[^/]+/([^/]+)/', 1)             AS workflow,
            regexp_extract(key, '^state/[^/]+/[^/]+/([^/]+)/', 1)       AS acquisition_date,
            regexp_extract(key, '^state/[^/]+/[^/]+/[^/]+/([^/]+)/', 1) AS source_granule_id,
            CAST(regexp_extract(key, '/([0-9]{{3}})$', 1) AS INT)        AS attempt,
            last_modified_date,
            key
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
          AND is_latest
          AND NOT is_delete_marker
        """
        # ruff: enable[E501]

        database_name = self.database.database_name
        assert database_name is not None

        view_spec = {
            "originalSql": sql,
            "catalog": Aws.ACCOUNT_ID,
            "schema": database_name,
            "columns": [
                {"name": col.name, "type": _athena_to_presto(col.type)}
                for col in _VIEW_COLUMNS
            ],
        }
        sql_b64 = base64.b64encode(json.dumps(view_spec).encode()).decode()

        view = glue.CfnTable(
            self,
            "StateView",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=view_name,
                table_type="VIRTUAL_VIEW",
                parameters={"presto_view": "true", "comment": "Presto View"},
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        type="string",
                        comment="Inventory report datetime.",
                    )
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=_VIEW_COLUMNS,
                    input_format=_SYMLINK_INPUT_FORMAT,
                    output_format=_HIVE_OUTPUT_FORMAT,
                ),
                view_original_text=f"/* Presto View: {sql_b64} */",
                view_expanded_text="/* Presto View */",
            ),
        )
        view.apply_removal_policy(RemovalPolicy.DESTROY)
        view.add_dependency(self.inventory_table)
        return view

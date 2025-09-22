import base64
import datetime as dt
import json
from typing import Any

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct


def athena_type_to_presto(athena_type: str | None) -> str:
    """Convert Athena column types to Presto types

    This only supports a relevant subset of types. See also:
    https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """
    # for type checking...
    if athena_type is None:
        raise ValueError("Cannot parse null athena type")

    lowered = athena_type.lower()
    return {
        "string": "varchar",
        "struct": "row",
        "float": "real",
        "binary": "varbinary",
    }.get(lowered, lowered)


class AthenaLogsDatabase(Construct):
    """Athena table for querying granule processing event logs"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database_name: str,
        table_datetime_start: dt.datetime,
        logs_s3_inventory_location_s3path: str,
        logs_s3_inventory_table_name: str,
        granule_processing_events_view_name: str,
        work_group_name: str | None = None,
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
                    "Database for HLS-VI Historical Orchestration task logging."
                ),
            ),
        )
        self.s3_inventory_table = self._create_s3_inventory_table(
            database=self.database,
            table_datetime_start=table_datetime_start,
            logs_s3_inventory_table_name=logs_s3_inventory_table_name,
            logs_s3_inventory_location_s3path=logs_s3_inventory_location_s3path,
        )
        self.granule_processing_events_view = (
            self._create_granule_processing_events_view(
                database=self.database,
                logs_s3_inventory_table_name=logs_s3_inventory_table_name,
                logs_s3_inventory_table=self.s3_inventory_table,
                granule_processing_events_view_name=granule_processing_events_view_name,
            )
        )

    def _create_s3_inventory_table(
        self,
        *,
        database: glue.CfnDatabase,
        table_datetime_start: dt.datetime,
        logs_s3_inventory_table_name: str,
        logs_s3_inventory_location_s3path: str,
    ) -> glue.CfnTable:
        """Create a Glue table for the S3 inventory reports"""
        database_name = database.database_name
        assert database_name is not None

        table = glue.CfnTable(
            self,
            "S3InventoryTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=logs_s3_inventory_table_name,
                table_type="EXTERNAL_TABLE",
                parameters={
                    "projection.dt.range": f"{table_datetime_start:%Y-%m-%d-%H-%M},NOW",
                    "projection.dt.interval.unit": "HOURS",
                    "EXTERNAL": "TRUE",
                    "projection.dt.type": "date",
                    "projection.enabled": "true",
                    "projection.dt.interval": "1",
                    "projection.dt.format": "yyyy-MM-dd-HH-mm",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        comment="Report date",
                        type="String",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                            name="bucket",
                            comment="The name of the bucket that the inventory is for.",
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="key",
                            comment=(
                                "The object key name (or key) that uniquely "
                                "identifies the object in the bucket."
                            ),
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="version_id",
                            comment="The object version ID.",
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="is_latest",
                            comment=(
                                "Set to True if the object is the current version "
                                "of the object."
                            ),
                            type="boolean",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="is_delete_marker",
                            comment="Set to True if the object is a delete marker.",
                            type="boolean",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="last_modified_date",
                            comment=(
                                "The object creation date or the last modified date, "
                                "whichever is the latest."
                            ),
                            type="timestamp",
                        ),
                    ],
                    location=logs_s3_inventory_location_s3path,
                    input_format="org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={
                            "serialization.format": "1",
                        },
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_dependency(database)
        return table

    def _create_granule_processing_events_view(
        self,
        *,
        database: glue.CfnDatabase,
        logs_s3_inventory_table_name: str,
        logs_s3_inventory_table: glue.CfnTable,
        granule_processing_events_view_name: str,
    ) -> glue.CfnTable:
        """Create a view for granule processing events logs"""
        database_name = database.database_name
        assert database_name is not None

        sql = rf"""
        SELECT
            regexp_extract(key, 'outcome=(\w+)', 1) as outcome,
            regexp_extract(key, 'platform=(\w+)', 1) as platform,
            date_parse(
                regexp_extract(key, 'acquisition_date=([\d-]+)', 1), '%Y-%m-%d'
            ) AS acquisition_date,
            regexp_extract(key, 'granule_id=([\w\.]+)', 1) AS granule_id,
            cast(regexp_extract(key, 'attempt=([0-9]+)', 1) AS INT) AS attempt,
            last_modified_date,
            key
        FROM {logs_s3_inventory_table_name}
        WHERE
            dt = (SELECT max(dt) FROM {logs_s3_inventory_table_name})
            AND is_latest
            AND NOT is_delete_marker
        """

        columns = [
            glue.CfnTable.ColumnProperty(
                name="outcome",
                comment="The processing event outcome (failed, success).",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="platform",
                comment="The platform (L30, S30).",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="acquisition_date",
                comment="The acquisition date.",
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="granule_id",
                comment="HLS granule ID.",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="attempt",
                comment="Attempt.",
                type="int",
            ),
            glue.CfnTable.ColumnProperty(
                name="last_modified_date",
                comment=(
                    "The event log's creation date or the last modified date, "
                    "whichever is the latest."
                ),
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="key",
                comment="The event log S3 key.",
                type="String",
            ),
        ]

        view_specification = {
            "originalSql": sql,
            "catalog": Aws.ACCOUNT_ID,
            "schema": database_name,
            "columns": [
                {"name": column.name, "type": athena_type_to_presto(column.type)}
                for column in columns
            ],
        }

        sql_b64 = base64.b64encode(
            json.dumps(view_specification).encode("utf-8")
        ).decode("utf-8")

        view = glue.CfnTable(
            self,
            "GranuleProcessingEventsView",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=granule_processing_events_view_name,
                table_type="VIRTUAL_VIEW",
                parameters={"presto_view": "true", "comment": "Presto View"},
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        comment="Report date",
                        type="String",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    input_format="org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                ),
                view_original_text=f"/* Presto View: {sql_b64} */",
                view_expanded_text="/* Presto View */",
            ),
        )
        view.apply_removal_policy(RemovalPolicy.DESTROY)
        view.add_dependency(logs_s3_inventory_table)
        return view

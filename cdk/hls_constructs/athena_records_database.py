"""CDK construct for the Athena records/ database.

Creates a Glue database + partition-projected tables over the three-object
S3 log schema's ``records/`` prefix.  Partition projection means new
date-partitions are queryable immediately as records land — no
``MSCK REPAIR TABLE`` or Glue crawler needed.

Currently creates one table:
  ``records_sentinel``  → s3://BUCKET/records/sentinel/

Stubs for ``records_landsat_ac`` and ``records_landsat_tile`` can be added
when Landsat Phase 1 is implemented.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct

# Athena JSON SerDe
_JSON_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
_JSON_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
_JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe"

# events[] struct — matches ProcessingEventRecord fields
_EVENTS_TYPE = "array<struct<state:string,ts:string,batch_job_id:string,exit_code:int>>"

_COLUMNS = [
    ("source_granule_id", "string", "Upstream SAFE / scene identifier"),
    ("output_granule_id", "string", "HLS output product identifier"),
    ("workflow", "string", "Processing workflow (sentinel, landsat-ac, landsat-tile)"),
    (
        "acquisition_date",
        "string",
        "Granule acquisition date YYYY-MM-DD (partition key)",
    ),
    ("attempt", "int", "Attempt number (0-indexed)"),
    ("batch_job_id", "string", "AWS Batch job ID"),
    (
        "shadow",
        "boolean",
        "True for Phase 0 shadow records from existing Step Functions",
    ),
    ("events", _EVENTS_TYPE, "Append-only list of state-transition events"),
    ("current_state", "string", "Most recent state"),
]


class AthenaRecordsDatabase(Construct):
    """Athena database for querying canonical processing records."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database_name: str,
        records_bucket_name: str,
        table_date_range_start: str,
        sentinel_table_name: str,
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
                    "Athena database for HLS NextGen canonical processing records."
                ),
            ),
        )

        self.sentinel_table = self._create_records_table(
            workflow="sentinel",
            table_name=sentinel_table_name,
            s3_location=f"s3://{records_bucket_name}/records/sentinel/",
            table_date_range_start=table_date_range_start,
        )

    def _create_records_table(
        self,
        *,
        workflow: str,
        table_name: str,
        s3_location: str,
        table_date_range_start: str,
    ) -> glue.CfnTable:
        columns = [
            glue.CfnTable.ColumnProperty(name=name, type=col_type, comment=comment)
            for name, col_type, comment in _COLUMNS
            if name != "acquisition_date"  # acquisition_date is a partition key
        ]

        # ruff: disable[E501]
        projection_params = {
            "EXTERNAL": "TRUE",
            "projection.enabled": "true",
            "projection.acquisition_date.type": "date",
            "projection.acquisition_date.format": "yyyy-MM-dd",
            "projection.acquisition_date.range": f"{table_date_range_start},NOW",
            "projection.acquisition_date.interval": "1",
            "projection.acquisition_date.interval.unit": "DAYS",
            "storage.location.template": f"{s3_location}${{acquisition_date}}/",
        }
        # ruff: enable[E501]

        table = glue.CfnTable(
            self,
            f"{workflow.capitalize()}RecordsTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=self.database.database_name,  # type: ignore[arg-type]
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters=projection_params,
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="acquisition_date",
                        type="string",
                        comment="Granule acquisition date YYYY-MM-DD",
                    )
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=s3_location,
                    input_format=_JSON_INPUT_FORMAT,
                    output_format=_JSON_OUTPUT_FORMAT,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=_JSON_SERDE,
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_dependency(self.database)
        return table

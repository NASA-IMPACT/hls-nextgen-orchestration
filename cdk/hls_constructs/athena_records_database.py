"""CDK construct for the Athena records/ database.

Creates a Glue database + partition-projected table over the Hive-style
``records/`` prefix.  Partition projection means new partitions are queryable
immediately as records land — no ``MSCK REPAIR TABLE`` or Glue crawler needed.

S3 key schema:
  ``records/workflow={workflow}/acquisition_date={acquisition_date}/source_granule_id={source_granule_id}/{attempt:03d}.json``

One table covers all workflows (sentinel, landsat-ac, landsat-tile) via the
``workflow`` enum partition — no per-workflow tables needed.

Also creates a ``granule_twin_status`` view that joins the records table on
``batch_job_id`` to expose ``n_granules_in_job`` / ``is_twin`` per row, with
partition pruning preserved through the join predicates.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct

from .athena_common import (
    HIVE_TEXT_OUTPUT_FORMAT,
    JSON_INPUT_FORMAT,
    JSON_SERDE,
    create_presto_view,
)

# events[] struct — matches ProcessingEventRecord fields
_EVENTS_TYPE = "array<struct<state:string,ts:string,batch_job_id:string,exit_code:int>>"

# workflow and acquisition_date are partition keys — excluded from body columns
_COLUMNS = [
    ("source_granule_id", "string", "Upstream SAFE / scene identifier"),
    ("output_granule_id", "string", "HLS output product identifier"),
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

_TWIN_VIEW_COLUMNS = [
    glue.CfnTable.ColumnProperty(name="source_granule_id", type="string"),
    glue.CfnTable.ColumnProperty(name="output_granule_id", type="string"),
    glue.CfnTable.ColumnProperty(name="workflow", type="string"),
    glue.CfnTable.ColumnProperty(name="acquisition_date", type="string"),
    glue.CfnTable.ColumnProperty(name="attempt", type="int"),
    glue.CfnTable.ColumnProperty(name="batch_job_id", type="string"),
    glue.CfnTable.ColumnProperty(name="current_state", type="string"),
    glue.CfnTable.ColumnProperty(name="shadow", type="boolean"),
    glue.CfnTable.ColumnProperty(name="n_granules_in_job", type="bigint"),
    glue.CfnTable.ColumnProperty(name="is_twin", type="boolean"),
]

_KNOWN_WORKFLOWS = ["sentinel", "landsat-ac", "landsat-tile"]


class AthenaRecordsDatabase(Construct):
    """Athena database for querying canonical processing records."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        records_bucket_name: str,
        table_date_range_start: str,
        table_name: str,
        twin_view_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = database

        s3_location = f"s3://{records_bucket_name}/records/"
        self.records_table = self._create_records_table(
            table_name=table_name,
            s3_location=s3_location,
            table_date_range_start=table_date_range_start,
            workflows=_KNOWN_WORKFLOWS,
        )
        self.twin_view = self._create_twin_view(
            view_name=twin_view_name,
            table_name=table_name,
            database_name=database_name,
        )

    def _create_records_table(
        self,
        *,
        table_name: str,
        s3_location: str,
        table_date_range_start: str,
        workflows: list[str],
    ) -> glue.CfnTable:
        columns = [
            glue.CfnTable.ColumnProperty(name=name, type=col_type, comment=comment)
            for name, col_type, comment in _COLUMNS
        ]

        # ruff: disable[E501]
        projection_params = {
            "EXTERNAL": "TRUE",
            "projection.enabled": "true",
            "projection.workflow.type": "enum",
            "projection.workflow.values": ",".join(workflows),
            "projection.acquisition_date.type": "date",
            "projection.acquisition_date.format": "yyyy-MM-dd",
            "projection.acquisition_date.range": f"{table_date_range_start},NOW",
            "projection.acquisition_date.interval": "1",
            "projection.acquisition_date.interval.unit": "DAYS",
            "storage.location.template": (
                f"{s3_location}workflow=${{workflow}}/acquisition_date=${{acquisition_date}}/"
            ),
        }
        # ruff: enable[E501]

        table = glue.CfnTable(
            self,
            "RecordsTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=self.database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters=projection_params,
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="workflow",
                        type="string",
                        comment=(
                            "Processing workflow (sentinel, landsat-ac, landsat-tile)"
                        ),
                    ),
                    glue.CfnTable.ColumnProperty(
                        name="acquisition_date",
                        type="string",
                        comment="Granule acquisition date YYYY-MM-DD",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=s3_location,
                    input_format=JSON_INPUT_FORMAT,
                    output_format=HIVE_TEXT_OUTPUT_FORMAT,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=JSON_SERDE,
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_dependency(self.database)
        return table

    def _create_twin_view(
        self, *, view_name: str, table_name: str, database_name: str
    ) -> glue.CfnTable:
        # ruff: disable[E501]
        sql = f"""
        SELECT
            r.source_granule_id,
            r.output_granule_id,
            r.workflow,
            r.acquisition_date,
            r.attempt,
            r.batch_job_id,
            r.current_state,
            r.shadow,
            t.n_granules_in_job,
            t.n_granules_in_job > 1   AS is_twin
        FROM "{table_name}" r
        JOIN (
            SELECT
                batch_job_id,
                workflow,
                acquisition_date,
                count(*) AS n_granules_in_job
            FROM "{table_name}"
            GROUP BY batch_job_id, workflow, acquisition_date
        ) t ON  r.batch_job_id    = t.batch_job_id
            AND r.workflow         = t.workflow
            AND r.acquisition_date = t.acquisition_date
        """
        # ruff: enable[E501]

        return create_presto_view(
            self,
            "TwinView",
            database=self.database,
            database_name=database_name,
            view_name=view_name,
            sql=sql,
            columns=_TWIN_VIEW_COLUMNS,
            depends_on=self.records_table,
        )

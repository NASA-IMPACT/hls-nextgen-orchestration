"""Athena view flagging the records of twin-granule jobs.

Joins the records table on ``batch_job_id`` to expose ``n_granules_in_job`` /
``is_twin`` per row, with partition pruning preserved through the join
predicates.
"""

from __future__ import annotations

from aws_cdk import aws_glue as glue
from batch_event_job_monitor_cdk.athena_common import create_presto_view
from constructs import Construct

_COLUMNS = [
    glue.CfnTable.ColumnProperty(name="input_entity_id", type="string"),
    glue.CfnTable.ColumnProperty(name="output_entity_id", type="string"),
    glue.CfnTable.ColumnProperty(name="job_type", type="string"),
    glue.CfnTable.ColumnProperty(name="acquisition_date", type="string"),
    glue.CfnTable.ColumnProperty(name="attempt", type="int"),
    glue.CfnTable.ColumnProperty(name="batch_job_id", type="string"),
    glue.CfnTable.ColumnProperty(name="current_state", type="string"),
    glue.CfnTable.ColumnProperty(name="n_granules_in_job", type="bigint"),
    glue.CfnTable.ColumnProperty(name="is_twin", type="boolean"),
]


def create_granule_twin_view(
    scope: Construct,
    construct_id: str,
    *,
    database: glue.CfnDatabase,
    database_name: str,
    records_table: glue.CfnTable,
    records_table_name: str,
    view_name: str,
) -> glue.CfnTable:
    """Create the ``granule_twin_status`` view over the records table."""
    sql = f"""
    SELECT
        r.input_entity_id,
        r.output_entity_id,
        r.job_type,
        r.acquisition_date,
        r.attempt,
        r.batch_job_id,
        r.current_state,
        t.n_granules_in_job,
        t.n_granules_in_job > 1   AS is_twin
    FROM "{records_table_name}" r
    JOIN (
        SELECT
            batch_job_id,
            job_type,
            acquisition_date,
            count(*) AS n_granules_in_job
        FROM "{records_table_name}"
        GROUP BY batch_job_id, job_type, acquisition_date
    ) t ON  r.batch_job_id     = t.batch_job_id
        AND r.job_type         = t.job_type
        AND r.acquisition_date = t.acquisition_date
    """
    return create_presto_view(
        scope,
        construct_id,
        database=database,
        database_name=database_name,
        view_name=view_name,
        sql=sql,
        columns=_COLUMNS,
        depends_on=records_table,
    )

from typing import Any

from aws_cdk import (
    Duration,
    aws_sqs as sqs,
)
from constructs import Construct


class QueueWithDlq(Construct):
    """SQS queue with a paired dead-letter queue.

    Both queues are created with SSL enforcement, SQS-managed encryption,
    and a 14-day retention period.

    Attributes
    ----------
    queue:
        The main queue.
    dlq:
        The dead-letter queue messages are moved to after ``max_receive_count``
        failed delivery attempts.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        queue_name: str,
        dlq_name: str,
        visibility_timeout: Duration,
        max_receive_count: int,
        retention_period: Duration = Duration.days(14),
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dlq = sqs.Queue(
            self,
            "DLQ",
            queue_name=dlq_name,
            retention_period=retention_period,
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        self.queue = sqs.Queue(
            self,
            "Queue",
            queue_name=queue_name,
            retention_period=retention_period,
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.dlq,
                max_receive_count=max_receive_count,
            ),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

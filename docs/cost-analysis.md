# Cost Analysis: S3 Event Store vs RDS/Postgres State DB

## Contents

- [Background](#background)
- [S3 Cost Estimate](#s3-cost-estimate)
- [RDS/Postgres Cost Estimate](#rdspostgres-cost-estimate)
- [Comparison](#comparison)
- [Scheduled/Poll vs Event-Driven](#scheduledpoll-vs-event-driven)
- [Risks](#risks)
- [Conclusion](#conclusion)

---

## Background

The orchestration system uses S3 as an event store and processing-state driver:

- **Canonical records**: one JSON file per granule per attempt, appended on each state transition
  (`records/workflow=<workflow>/acquisition_date=<date>/source_granule_id=<id>/`)
- **State pointers**: lightweight marker objects whose key encodes the current state, written and deleted on each
  transition (`state/state=<state>/workflow=<workflow>/acquisition_date=<date>/source_granule_id=<id>/<attempt>`)
- **Output index**: empty marker objects for terminal-state lookups
  (`outputs/state=<state>/workflow=<workflow>/acquisition_date=<date>/<output_id>`)
- **Analytical layer** — Athena + S3 inventory for reconciliation and throughput queries

The alternative is a relational DB (Postgres / RDS / Aurora), which is the pattern used in the existing Step Functions
pipeline.

---

## S3 Cost Estimate

### Assumptions

| Parameter                     | Value       | Notes                                                                |
| ----------------------------- | ----------- | -------------------------------------------------------------------- |
| Granules processed            | ~15,000/day | Landsat ~5,500-6,000 + Sentinel-2 ~8,500-9,000                       |
| State transitions per granule | ~4-5        | AWAITING, SUBMITTED, SUCCESS/FAILED, plus retries                    |
| S3 API calls per transition   | ~6          | GET + PUT for canonical record, PUT + DELETE + PUT for state pointer |
| Average record size           | ~1 KB       | Small JSON with event array                                          |

### Steady-state operational cost (per year)

| Cost driver                      | Volume                               | Rate                    | Annual cost    |
| -------------------------------- | ------------------------------------ | ----------------------- | -------------- |
| S3 PUT requests                  | ~90k/day, 33M/yr                     | $0.005/1,000            | ~$165          |
| S3 GET requests                  | ~30k/day, 11M/yr                     | $0.0004/1,000           | ~$4            |
| S3 LIST requests (list_awaiting) | ~1,440/day, 525k/yr                  | $0.005/1,000            | ~$3            |
| Storage (records accumulate)     | ~5-6 GB/yr growth                    | $0.023/GB/month         | <$8/yr         |
| Athena queries (rare analysis)   | Negligible with partition projection | $5/TB scanned           | ~$0            |
| S3 inventory                     | ~1 report/day                        | $0.0025/million objects | ~$1            |
| **Total**                        |                                      |                         | **~$180/year** |

### One-time full historical reprocessing

Sentinel-2 has produced roughly 20 million acquisitions since 2015. At 6 API calls per transition and 4 transitions per
granule:

| Item                     | Estimate                  |
| ------------------------ | ------------------------- |
| S3 API calls             | ~480M                     |
| PUT cost                 | ~$2,400                   |
| GET/LIST cost            | ~$300                     |
| Storage for full history | ~20 GB, ~$5/month ongoing |
| **One-time total**       | **~$2,700**               |

Twin granule jobs add 20-30% overhead on affected tiles. The cost impact is minor.

---

## RDS/Postgres Cost Estimate

Best-case assumption: Aurora Serverless v2 with `min_capacity = 0` and auto-pause enabled. Incoming granules arrive
continuously from Landsat and Sentinel-2 ground stations throughout the day, so the DB is active almost all the time and
auto-pause rarely fires. Processing itself is bursty (triggered when ancillary data arrives), so ACU demand is bimodal:
low but non-zero for continuous ingest writes, spiking during ancillary processing windows. A realistic estimate is ~22
active hours/day at a blended 1.5 ACU average.

| Item                                          | Rate           | Annual cost      |
| --------------------------------------------- | -------------- | ---------------- |
| Aurora Serverless v2 (22 hr/day, 1.5 ACU avg) | $0.12/ACU-hr   | ~$1,450          |
| Storage (Aurora, 50 GB)                       | $0.10/GB/month | ~$60/yr          |
| I/O                                           | $0.20/million  | ~$30/yr          |
| **Total**                                     |                | **~$1,540/year** |

Note: the existing production pipeline runs at `min_capacity = 4` ACU with no auto-pause, which costs ~$375/month
(~$4,500/year). The $1,540 figure above is what an optimised deployment would cost.

The existing pipeline prunes records to keep the DB manageable. Pruning adds developer time but storage cost is low
enough that this is mainly about query performance, not cost.

---

## Comparison

|                              | S3 event store               | RDS/Postgres                            |
| ---------------------------- | ---------------------------- | --------------------------------------- |
| Steady-state annual cost     | ~$180                        | ~$1,540 (best case, auto-pause enabled) |
| Full historical reprocessing | ~$2,700 one-time             | Instance cost only                      |
| Indefinite retention         | Yes, cost scales with volume | Requires pruning or partitioning        |
| Storage cost per GB          | $0.023                       | $0.115 (5x more)                        |
| Write cost                   | Per-operation                | Included in instance                    |
| Analytical query latency     | 10-30s (Athena cold start)   | Milliseconds (but contends with writes) |
| Real-time state visibility   | 24-hour lag via S3 inventory | Live                                    |
| Operational complexity       | Higher                       | Lower                                   |

S3 is cheaper at HLS granule volumes. RDS becomes cost-competitive only if analytical query volume is high enough that
the per-operation S3 cost exceeds the fixed instance cost. That does not happen at current or projected HLS scale.

---

## Scheduled/Poll vs Event-Driven

The existing Step Functions pipeline uses a scheduled or poll-based model: a state machine wakes up on a timer, checks
for work, dispatches jobs, and polls for completion. The new design is event-driven: S3 notifications and EventBridge
rules fire only when something actually happens.

### Where polling costs accumulate

**Step Functions Standard Workflows** charge $0.025 per 1,000 state transitions. A typical job moves through 10-15
states (submit, wait, retry, succeed/fail). At 10,000 jobs/day and 12 transitions per job:

| Item              | Volume           | Rate         | Annual cost |
| ----------------- | ---------------- | ------------ | ----------- |
| State transitions | 120k/day, 44M/yr | $0.025/1,000 | ~$1,100     |

Express Workflows are much cheaper ($1.00/million transitions + duration), but the Standard Workflow pricing is what the
existing pipeline uses.

**Scheduled Lambda polling** (e.g., check for new work every minute) runs 1,440 invocations/day per function regardless
of whether there is work to do. Lambda invocation cost is negligible ($0.20/million), but the pattern means you are
always paying for no-ops. More importantly, polling introduces latency: a granule that arrives 1 second after a poll
waits up to 59 seconds to be picked up.

**Repeated AWS API calls** add up at scale. Polling Batch `DescribeJobs` for 10,000 active jobs at once requires
paginated calls every poll cycle. These are not billed directly but contribute to throttling risk and Lambda duration
cost.

### Event-driven costs

| Trigger                              | Volume at 10k granules/day | Rate          | Annual cost   |
| ------------------------------------ | -------------------------- | ------------- | ------------- |
| S3 event notifications               | 10k/day (free to SQS)      | $0            | $0            |
| SQS messages (send + receive)        | ~40k/day                   | $0.40/million | ~$6           |
| EventBridge (Batch job events)       | ~10k/day                   | $1.00/million | ~$3.65        |
| Lambda invocations (event-triggered) | ~20k/day                   | $0.20/million | ~$1.50        |
| **Total**                            |                            |               | **~$11/year** |

Lambda only runs when there is a real event. There are no no-op poll cycles.

### Comparison

|                            | Scheduled/poll                             | Event-driven             |
| -------------------------- | ------------------------------------------ | ------------------------ |
| Step Functions             | ~$1,100/year (Standard)                    | Not used                 |
| Lambda idle cost           | Constant (1,440+ invocations/day/function) | Zero                     |
| Trigger latency            | Up to poll interval (seconds to minutes)   | Near-zero (milliseconds) |
| Scales with granule volume | No (polling is fixed cost)                 | Yes                      |
| Throttling risk            | Higher (repeated describe calls)           | Lower                    |
| **Approx. annual cost**    | **~$1,100+**                               | **~$11**                 |

The event-driven approach is roughly 100x cheaper for the orchestration layer at HLS scale. The saving comes from two
sources: removing Step Functions Standard Workflow charges, and eliminating idle Lambda invocations that find no work to
do.

---

## Risks

**RDS analytics contention.** The existing pipeline already sees write timeouts under load. Running analytical queries
against the same cluster makes this worse -- more connections and longer-running reads compete with writes for ACU
capacity and connection slots. Read replicas help but add cost. Athena over S3 sidesteps this entirely -- analytical
queries never touch the write path.

**Real-time monitoring.** The main gap is operational visibility. Athena's cold query time is too slow for live
dashboards. The S3 inventory covers reconciliation with a 24-hour lag, which works for most use cases. If sub-second
"how many granules are currently SUBMITTED?" queries become necessary, a lightweight DynamoDB or Redis cache for
current-state counts is the right fix. That is a targeted addition, not a full migration to RDS.

**Write amplification at high throughput.** Each canonical record append is a non-atomic GET + PUT. At high burst rates
during historical reprocessing, this generates significant S3 API traffic. The numbers above account for this. Monitor
API costs during large reprocessing runs.

**Developer complexity.** S3 + Athena is harder to operate than a standard Postgres schema. This analysis excludes
developer time. The S3 approach trades lower infrastructure cost for higher complexity in the state management layer.

**Twin granule overhead.** Twin granule jobs (8x more common than originally estimated) write records for each source
granule. This roughly doubles API cost for affected tiles. It does not change the overall conclusion.

---

## Conclusion

For HLS access patterns (high write volume, rare reads, indefinite retention, no real-time query requirement) the S3
event store is cheaper than RDS. The main saving is removing the always-on instance cost of $300-600/year. S3 costs
scale with granule volume and stay low at operational scale.

The assumption holds unless real-time operational monitoring becomes a hard requirement. In that case, add a lightweight
cache rather than migrating to RDS.

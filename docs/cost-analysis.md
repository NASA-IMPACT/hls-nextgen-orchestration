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
- **Analytical layer** -- Athena + S3 inventory for reconciliation and throughput queries

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

### One-time full historical reprocessing (HLS v3, ~30M granules)

| Item                       | New system (S3 + event-driven) | Old system (Step Functions + RDS) |
| -------------------------- | ------------------------------ | --------------------------------- |
| S3 API calls (720M total)  | ~$4,100                        | --                                 |
| Step Functions transitions | --                              | ~$55,000-$85,000                  |
| Storage ongoing            | ~30 GB, ~$1/month              | Requires pruning                  |
| **One-time total**         | **~$4,100**                    | **~$55,000-$85,000**              |

The S3 cost scales linearly: 30M granules x 4 transitions x 6 API calls = 720M calls at the same per-request rates as
steady-state operations.

The Step Functions range reflects whether ancillary data is pre-available for the historical granules. If it is (the
expected case for a reprocessing run), the 24-36 hour ancillary-wait polling loop is skipped, dropping to ~72
transitions/granule (~$55,000). At the full production rate of 113 transitions/granule the cost reaches ~$85,000.
Switching to Express Workflows would reduce this to ~$26,000, but requires code changes.

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

**Step Functions Standard Workflows** charge $0.025 per 1,000 state transitions. A naive count of job lifecycle states
(submit, wait, retry, succeed/fail) gives ~12 transitions per job, but the polling loop dominates in practice. Each
Batch status check is a `Wait -> Lambda -> Choice` chain -- 3 transitions per cycle. A job that takes 20-30 minutes polled
every 30-60 seconds runs 20-60 such loops, adding 60-180 polling transitions on top of the ~12 real ones.

Back-calculating from actual spend ($15,517.69/year at $0.025/1,000) gives ~1.7M transitions/day, or ~113 per granule at
15,000 granules/day. That is consistent with ~20-minute average Batch job runtimes polled every 30-60 seconds.

| Item                           | Volume            | Rate         | Annual cost        |
| ------------------------------ | ----------------- | ------------ | ------------------ |
| State transitions              | 1.7M/day, 621M/yr | $0.025/1,000 | **$15,518 actual** |
| (naive 12-transition estimate) | 180k/day, 66M/yr  | $0.025/1,000 | ~$1,640            |

Express Workflows are much cheaper ($1.00/million transitions + duration), but the Standard Workflow pricing is what the
existing pipeline uses.

**Lambda polling for ancillary data** runs inside each Step Functions execution: a Lambda checks whether the required
ancillary data have arrived; if not, the state machine waits one hour and retries. Invocation count scales with how long
granules sit waiting, not with a fixed global schedule. At ~100ms per check, individual duration cost is negligible, but
invocation count accumulates: a granule waiting 24-36 hours for ancillary data triggers 24-36 retries. Across 15,000
granules/day that is tens of millions of invocations per year, which accounts for the actual Lambda spend of ~$205/year.

**Repeated AWS API calls** add up at scale. Polling Batch `DescribeJobs` for 15,000 active jobs at once requires
paginated calls every poll cycle. These are not billed directly but contribute to throttling risk and Lambda duration
cost.

### Event-driven costs

| Trigger                              | Volume at 15k granules/day | Rate          | Annual cost   |
| ------------------------------------ | -------------------------- | ------------- | ------------- |
| S3 event notifications               | 15k/day (free to SQS)      | $0            | $0            |
| SQS messages (send + receive)        | ~60k/day                   | $0.40/million | ~$9           |
| EventBridge (Batch job events)       | ~15k/day                   | $1.00/million | ~$5.50        |
| Lambda invocations (event-triggered) | ~30k/day                   | $0.20/million | ~$2           |
| **Total**                            |                            |               | **~$16/year** |

Lambda only runs when there is a real event. There are no no-op poll cycles.

### Comparison

|                       | Scheduled/poll (existing)                | Event-driven (new) |
| --------------------- | ---------------------------------------- | ------------------ |
| Step Functions        | **$15,518/year actual**                  | Not used           |
| RDS/Postgres          | **$4,375/year actual** (min_cap=4)       | Not used           |
| Lambda                | **$205/year actual** (invocation-driven) | ~$2/year           |
| S3 (ancillary checks) | **~$60/year actual** (List/Head calls)   | ~$0                |
| S3 event store        | Not used                                 | ~$180/year         |
| SQS + EventBridge     | Not used                                 | ~$15/year          |
| **Total annual cost** | **~$20,160/year**                        | **~$200/year**     |

The event-driven approach is roughly 100x cheaper at HLS scale. The dominant saving is Step Functions: polling loops
generate ~113 state transitions per granule versus the ~12 "real" job-lifecycle transitions, inflating cost by ~10x over
a naive estimate. Removing RDS adds another ~$4,400/year saving. The existing system also spends ~$60/year on S3
List/Head calls for ancillary data checks; the new design eliminates these by reacting to S3 events instead of polling.

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

The throttling risk is mitigated by the key prefix design. S3 scales to at least 3,500 PUT and 5,500 GET requests per
second [per prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html). The
hive-partitioned layout (`state=<state>/workflow=<workflow>/acquisition_date=<date>/source_granule_id=<id>/`) spreads
writes across a large number of distinct prefixes -- one per granule per acquisition date -- so even at peak reprocessing
throughput the per-prefix request rate stays well below the S3 limit.

**Developer complexity.** S3 + Athena is harder to operate than a standard Postgres schema. This analysis excludes
developer time. The S3 approach trades lower infrastructure cost for higher complexity in the state management layer.

**Twin granule overhead.** Twin granule jobs (8x more common than originally estimated) write records for each source
granule. This roughly doubles API cost for affected tiles. It does not change the overall conclusion.

---

## Conclusion

For HLS access patterns (high write volume, rare reads, indefinite retention, no real-time query requirement) the S3
event store is cheaper than RDS. The dominant saving is eliminating Step Functions: polling loops generate ~100x more
state transitions than the job lifecycle alone, making it by far the largest cost driver in the existing system. S3
costs scale with granule volume and stay low at operational scale.

The assumption holds unless real-time operational monitoring becomes a hard requirement. In that case, add a lightweight
cache rather than migrating to RDS.

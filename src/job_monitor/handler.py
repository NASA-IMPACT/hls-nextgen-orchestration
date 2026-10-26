"""HLS job monitor Lambda.

BEJM's job monitor, with an untracked-job resolver that identifies the
existing (Phase 0) system's jobs, which carry no bejm_* parameters. Jobs this
system submits carry them and are decoded from them.
"""

from batch_event_job_monitor.handlers.job_monitor_handler import make_handler

from common.shadow import resolve_phase0_job

handler = make_handler(resolve_untracked=resolve_phase0_job)

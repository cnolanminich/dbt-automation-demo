"""Safe Scheduled Job Component - skips execution if selected assets are already running.

This component creates a schedule that dynamically checks if any of the selected
assets are currently being materialized before launching a new run. If assets are
running, the schedule tick is skipped to prevent overlapping executions.
"""

from dataclasses import dataclass

import dagster as dg


@dataclass
class SafeScheduledJobComponent(dg.Component, dg.Resolvable):
    """Component for scheduling assets with run-in-progress protection.

    Before each scheduled tick, this component checks if any of the selected
    assets are currently being materialized. If so, the schedule skips that tick.

    This is useful for:
    - Preventing overlapping runs of long-running assets
    - Avoiding resource contention
    - Ensuring data consistency when runs shouldn't overlap

    Attributes:
        job_name: Name of the job to create
        cron_schedule: Cron expression for the schedule (e.g., "0 * * * *" for hourly)
        asset_selection: Asset selection string (e.g., "tag:schedule=hourly", "kind:dbt")
        skip_reason_prefix: Custom prefix for skip messages (optional)
    """

    job_name: str
    cron_schedule: str
    asset_selection: str
    skip_reason_prefix: str = "Skipping scheduled run"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build a scheduled job with run-in-progress protection."""

        # Define the job with the asset selection
        job = dg.define_asset_job(
            name=self.job_name,
            selection=self.asset_selection,
        )

        # Capture attributes for closure
        job_name = self.job_name
        asset_selection = self.asset_selection
        skip_reason_prefix = self.skip_reason_prefix
        schedule_name = f"{job_name}_schedule"

        def schedule_fn(context: dg.ScheduleEvaluationContext):
            """Schedule that checks for running assets before execution.

            This function queries the Dagster instance to check if any runs
            targeting the selected assets are currently in progress. If so,
            it skips this tick to prevent overlapping executions.
            """
            # Get the Dagster instance to query run status
            instance = context.instance

            # Query for in-progress runs
            in_progress_runs = instance.get_runs(
                filters=dg.RunsFilter(
                    statuses=[
                        dg.DagsterRunStatus.STARTED,
                        dg.DagsterRunStatus.STARTING,
                        dg.DagsterRunStatus.QUEUED,
                    ],
                ),
                limit=100,  # Check recent runs
            )

            # Check if any in-progress runs target our job or overlap with our assets
            for run in in_progress_runs:
                # Check if it's the same job
                if run.job_name == job_name:
                    context.log.info(
                        f"{skip_reason_prefix}: Job '{job_name}' is already running "
                        f"(run_id: {run.run_id}, status: {run.status})"
                    )
                    return dg.SkipReason(
                        f"Job '{job_name}' is already running (run_id: {run.run_id})"
                    )

                # Check if run targets any of the same assets
                # The run's asset_selection is stored in tags
                run_asset_selection = run.tags.get("dagster/asset_selection")
                if run_asset_selection and _selections_overlap(
                    asset_selection, run_asset_selection
                ):
                    context.log.info(
                        f"{skip_reason_prefix}: Assets in selection '{asset_selection}' "
                        f"are being materialized by run '{run.run_id}'"
                    )
                    return dg.SkipReason(
                        f"Assets are already being materialized (run_id: {run.run_id})"
                    )

            # No conflicts found, proceed with the run
            context.log.info(
                f"No conflicting runs found. Launching job '{job_name}' "
                f"with selection '{asset_selection}'"
            )
            return dg.RunRequest(run_key=None)

        # Create the schedule with a unique name
        schedule = dg.ScheduleDefinition(
            name=schedule_name,
            job=job,
            cron_schedule=self.cron_schedule,
            execution_fn=schedule_fn,
        )

        return dg.Definitions(
            schedules=[schedule],
            jobs=[job],
        )


def _selections_overlap(selection1: str, selection2: str) -> bool:
    """Check if two asset selections might overlap.

    This is a simple heuristic check. For exact overlap detection,
    you would need to resolve both selections against the asset graph.

    For now, we check if:
    - The selections are identical
    - One selection is a subset pattern of the other
    """
    # Normalize selections
    s1 = selection1.strip().lower()
    s2 = selection2.strip().lower()

    # Exact match
    if s1 == s2:
        return True

    # Check if they share common tags/groups/kinds
    # This is a heuristic - for production, resolve against asset graph
    s1_parts = set(s1.replace("|", " ").split())
    s2_parts = set(s2.replace("|", " ").split())

    # If any selection terms match, assume overlap
    return bool(s1_parts & s2_parts)

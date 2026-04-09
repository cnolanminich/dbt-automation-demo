"""Scheduled Job Component - creates a schedule targeting an asset selection."""

from dataclasses import dataclass

import dagster as dg


@dataclass
class ScheduledJobComponent(dg.Component, dg.Resolvable):
    """Component for scheduling asset materialization jobs.

    Attributes:
        job_name: Name of the job to create
        cron_schedule: Cron expression for the schedule (e.g., "0 * * * *" for hourly)
        asset_selection: Asset selection string (e.g., "tag:schedule=hourly", "kind:dbt")
    """

    job_name: str
    cron_schedule: str
    asset_selection: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build a scheduled job."""
        job = dg.define_asset_job(
            name=self.job_name,
            selection=self.asset_selection,
        )

        schedule = dg.ScheduleDefinition(
            name=f"{self.job_name}_schedule",
            job=job,
            cron_schedule=self.cron_schedule,
        )

        return dg.Definitions(
            schedules=[schedule],
            jobs=[job],
        )

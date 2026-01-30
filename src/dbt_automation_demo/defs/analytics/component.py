"""Custom dbt component with materialization-based automation policies.

Automation Policy Strategy:
- VIEWS (staging models): on_missing | code_version_changed | newly_updated
  - Views are cheap to recreate, so we refresh them when missing, code changes, or upstream updates
- TABLES with 'refresh_2min' tag: on_cron every 2 minutes
  - For staging tables that need frequent refresh
- NON-VIEW dbt models (tables in marts): on_cron daily at 6 AM UTC
  - Ignores view dependencies (views update reactively)
  - Checks if deps updated since midnight (not since cron tick)
- Other non-view assets: on_cron hourly

Each asset is tagged with `dagster/materialization` (view, table, incremental, etc.)
to enable programmatic selection in schedules and jobs.
"""

from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DbtProjectComponent
from dagster_dbt.dbt_project import DbtProject


@dataclass
class CustomDbtComponent(DbtProjectComponent):
    """Custom dbt component with materialization-based automation policies.

    This component automatically:
    1. Tags each model with `dagster/materialization` based on dbt config
    2. Assigns different automation conditions:
       - Views: on_missing | code_version_changed | newly_updated
       - Tables with 'refresh_2min' dbt tag: on_cron every 2 minutes
       - Mart tables: on_cron daily (6 AM UTC), ignores view deps, midnight lookback
       - Other tables: on_cron hourly

    All cron-based automations include ~in_progress() to prevent overlapping runs.

    The `dagster/materialization` tag enables programmatic asset selection:
    - Select tables only: `tag:dagster/materialization=table`
    - Exclude views: `- tag:dagster/materialization=view`
    """

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: Optional[DbtProject]
    ) -> dg.AssetSpec:
        """Override to apply materialization-based automation conditions and tags."""
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        # Get the node properties
        node = self.get_resource_props(manifest, unique_id)
        materialized = node.get("config", {}).get("materialized", "view")
        resource_type = node.get("resource_type", "model")
        fqn = node.get("fqn", [])
        dbt_tags = node.get("tags", [])  # dbt model tags

        # Only apply automation to models (not seeds, sources, etc.)
        if resource_type != "model":
            return base_spec

        # Add materialization tag for programmatic selection
        # This enables selections like:
        #   - "tag:dagster/materialization=table" (tables only)
        #   - "kind:dbt - tag:dagster/materialization=view" (dbt assets excluding views)
        existing_tags = dict(base_spec.tags) if base_spec.tags else {}
        existing_tags["dagster/materialization"] = materialized

        # Determine the automation condition based on materialization and tags
        if materialized == "view":
            # Views: refresh when missing, on code version change, or when upstream updates
            # on_missing() is critical for new assets that have never been materialized
            automation_condition = (
                dg.AutomationCondition.on_missing()
                | dg.AutomationCondition.code_version_changed()
                | dg.AutomationCondition.newly_updated()
            )
        elif "refresh_2min" in dbt_tags:
            # Staging tables with refresh_2min tag: every 2 minutes
            # These are frequently-updated tables that feed into downstream marts
            automation_condition = (
                dg.AutomationCondition.on_cron("*/2 * * * *").ignore(
                    dg.AssetSelection.tag("dagster/materialization", "view")
                )
                & ~dg.AutomationCondition.in_progress()
            )
        elif "marts" in fqn:
            # Mart tables (analytics): daily refresh at 6 AM UTC
            # - Ignores view dependencies (views refresh reactively via newly_updated)
            # - Uses midnight lookback for dependency check (not 6 AM)
            #   This allows upstream tables that run between midnight and 6 AM to count
            automation_condition = (
                dg.AutomationCondition.cron_tick_passed("* * * * *")  # Trigger at 6 AM
                & dg.AutomationCondition.all_deps_updated_since_cron("0 0 * * *").ignore(
                    dg.AssetSelection.tag("dagster/materialization", "view")  # Ignore views
                )
                & ~dg.AutomationCondition.in_progress()
            )
        else:
            # Other non-view models: hourly refresh
            # - Ignores view dependencies
            automation_condition = (
                dg.AutomationCondition.on_cron("* * * * *").ignore(
                    dg.AssetSelection.tag("dagster/materialization", "view")
                )
                & ~dg.AutomationCondition.in_progress()
            )

        return base_spec.replace_attributes(
            automation_condition=automation_condition,
            tags=existing_tags,
        )

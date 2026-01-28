"""Custom dbt component with materialization-based automation policies.

Automation Policy Strategy:
- VIEWS (staging models): code_version_changed | newly_updated
  - Views are cheap to recreate, so we refresh them when code changes
- NON-VIEW dbt models (tables in marts): on_cron daily at 6 AM UTC
- Other non-view assets: on_cron hourly
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

    This component automatically assigns different automation conditions:
    - Views: code_version_changed | newly_updated
    - Mart tables: on_cron daily (6 AM UTC)
    - Other tables: on_cron hourly

    All cron-based automations include ~in_progress() to prevent overlapping runs.
    """

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: Optional[DbtProject]
    ) -> dg.AssetSpec:
        """Override to apply materialization-based automation conditions."""
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        # Get the node properties
        node = self.get_resource_props(manifest, unique_id)
        materialized = node.get("config", {}).get("materialized", "view")
        resource_type = node.get("resource_type", "model")
        fqn = node.get("fqn", [])

        # Only apply automation to models (not seeds, sources, etc.)
        if resource_type != "model":
            return base_spec

        # Determine the automation condition based on materialization
        if materialized == "view":
            # Views: refresh on code version change or when upstream updates
            automation_condition = (
                dg.AutomationCondition.code_version_changed()
                | dg.AutomationCondition.newly_updated()
            )
        elif "marts" in fqn:
            # Mart tables (analytics): daily refresh at 6 AM UTC
            automation_condition = (
                dg.AutomationCondition.on_cron("0 6 * * *")
                & ~dg.AutomationCondition.in_progress()
            )
        else:
            # Other non-view models: hourly refresh
            automation_condition = (
                dg.AutomationCondition.on_cron("0 * * * *")
                & ~dg.AutomationCondition.in_progress()
            )

        return base_spec.replace_attributes(
            automation_condition=automation_condition
        )

# dbt Automation Demo

This Dagster project demonstrates **materialization-based declarative automation** for dbt assets, showing how to apply different automation policies based on whether a dbt model is a view or a table.

## Key Concepts Demonstrated

1. **Declarative Automation Conditions** - Different refresh strategies based on asset type
2. **Programmatic Materialization Tagging** - Auto-tagging assets with `dagster/materialization` for selection
3. **Custom dbt Component** - Subclassing `DbtProjectComponent` to customize automation
4. **Safe Scheduled Jobs** - A schedule that checks for running assets before execution
5. **YAML Consolidation** - Multiple component definitions in a single file using `---` separators

## Automation Strategy

| Asset Type | Materialization | Automation Condition | Behavior |
|------------|-----------------|---------------------|----------|
| Staging models | `view` | `on_missing \| code_version_changed \| newly_updated` | Refresh when missing (new), SQL code changes, OR when upstream seeds are updated |
| Mart models | `table` | `cron_tick_passed("* * * * *") & all_deps_updated_since_cron("*/10 * * * *")` | Every minute if deps updated in last 10 min |

### Why Different Policies?

- **Views** are cheap to recreate (they're just stored queries), so refreshing them on code changes makes sense. The `newly_updated` condition also triggers them when upstream dependencies are materialized.

- **Tables** are expensive to rebuild and typically don't need to refresh every time code changes during development. A daily cron schedule is more appropriate for production analytics tables.

### Understanding the Automation Conditions

```python
# For views: refresh reactively
dg.AutomationCondition.on_missing() | dg.AutomationCondition.code_version_changed() | dg.AutomationCondition.newly_updated()
```

- `on_missing()` - Triggers when the asset has never been materialized (critical for new assets!)
- `code_version_changed()` - Triggers when the asset's code (SQL) changes
- `newly_updated()` - Triggers when any upstream dependency is materialized
- `|` (OR) - Any condition triggers a refresh

```python
# For tables: refresh on schedule
dg.AutomationCondition.on_cron("0 6 * * *") & ~dg.AutomationCondition.in_progress()
```

- `on_cron("0 6 * * *")` - Triggers at 6 AM UTC daily
- `~in_progress()` - Only if the asset is NOT currently being materialized
- `&` (AND) - Both conditions must be true

**Important:** `on_cron()` does NOT include `~in_progress()` by default. You must add it explicitly to prevent overlapping runs.

### Programmatic Materialization Tagging

Each dbt model is automatically tagged with `dagster/materialization` based on its dbt config:

| dbt Materialization | Dagster Tag |
|---------------------|-------------|
| `view` | `dagster/materialization=view` |
| `table` | `dagster/materialization=table` |
| `incremental` | `dagster/materialization=incremental` |
| `ephemeral` | `dagster/materialization=ephemeral` |

This enables **programmatic asset selection** in schedules and jobs:

```yaml
# Select only tables (exclude views)
asset_selection: "tag:dagster/materialization=table"

# Select all dbt assets except views
asset_selection: "kind:dbt - tag:dagster/materialization=view"

# Select views only
asset_selection: "tag:dagster/materialization=view"
```

This approach is more robust than folder-based selection (`tag:marts`) because it uses the actual dbt materialization config, not the directory structure.

## Project Structure

```
dbt-automation-demo/
├── analytics_dbt/                    # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml                  # DuckDB in-memory config
│   ├── seeds/
│   │   ├── raw_customers.csv         # Sample customer data
│   │   └── raw_orders.csv            # Sample order data
│   └── models/
│       ├── staging/
│       │   ├── stg_customers.sql     # VIEW - code_version_changed | newly_updated
│       │   └── stg_orders.sql        # VIEW - code_version_changed | newly_updated
│       └── marts/
│           ├── customer_orders.sql   # TABLE - on_cron daily
│           ├── customer_segments.sql # TABLE - on_cron daily
│           └── order_summary.sql     # TABLE - on_cron daily
│
├── src/dbt_automation_demo/
│   ├── definitions.py                # Dagster entry point
│   └── defs/
│       ├── analytics/
│       │   ├── component.py          # Custom dbt component with automation logic
│       │   └── defs.yaml             # Component instances (consolidated)
│       └── components/
│           └── safe_scheduled_job_component.py  # Reusable schedule component
│
└── pyproject.toml
```

## Asset Lineage

```
┌─────────────────┐     ┌──────────────┐
│  raw_customers  │     │  raw_orders  │
│     (seed)      │     │    (seed)    │
└────────┬────────┘     └──────┬───────┘
         │                     │
         ▼                     ▼
┌─────────────────┐     ┌──────────────┐
│  stg_customers  │     │  stg_orders  │
│     (view)      │     │    (view)    │
└────────┬────────┘     └──────┬───────┘
         │                     │
         │    ┌────────────────┤
         │    │                │
         ▼    ▼                ▼
┌─────────────────┐     ┌──────────────┐
│ customer_orders │     │order_summary │
│    (table)      │     │   (table)    │
└────────┬────────┘     └──────────────┘
         │
         ▼
┌─────────────────┐
│customer_segments│
│    (table)      │
└─────────────────┘
```

## Key Components

### 1. CustomDbtComponent (`defs/analytics/component.py`)

This component subclasses `DbtProjectComponent` and overrides `get_asset_spec()` to apply different automation conditions based on the dbt model's materialization:

```python
@dataclass
class CustomDbtComponent(DbtProjectComponent):
    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: Optional[DbtProject]
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        node = self.get_resource_props(manifest, unique_id)
        materialized = node.get("config", {}).get("materialized", "view")

        # Add materialization tag for programmatic selection
        existing_tags = dict(base_spec.tags) if base_spec.tags else {}
        existing_tags["dagster/materialization"] = materialized

        if materialized == "view":
            # Views: refresh when missing, on code change, or upstream update
            automation_condition = (
                dg.AutomationCondition.on_missing()
                | dg.AutomationCondition.code_version_changed()
                | dg.AutomationCondition.newly_updated()
            )
        else:
            # Tables: daily cron, skip if running
            automation_condition = (
                dg.AutomationCondition.on_cron("0 6 * * *")
                & ~dg.AutomationCondition.in_progress()
            )

        return base_spec.replace_attributes(
            automation_condition=automation_condition,
            tags=existing_tags,
        )
```

### 2. SafeScheduledJobComponent (`defs/components/safe_scheduled_job_component.py`)

A reusable component that creates a schedule with run-in-progress protection. Before each tick, it queries the Dagster instance for running jobs and skips if there's a conflict.

**Note:** This is separate from declarative automation. Use this when you want explicit schedule-based triggering rather than condition-based automation.

```python
@dataclass
class SafeScheduledJobComponent(dg.Component, dg.Resolvable):
    job_name: str
    cron_schedule: str
    asset_selection: str
    skip_reason_prefix: str = "Skipping scheduled run"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        job = dg.define_asset_job(name=self.job_name, selection=self.asset_selection)

        def schedule_fn(context: dg.ScheduleEvaluationContext):
            # Check for in-progress runs before triggering
            in_progress_runs = context.instance.get_runs(
                filters=dg.RunsFilter(
                    statuses=[
                        dg.DagsterRunStatus.STARTED,
                        dg.DagsterRunStatus.STARTING,
                        dg.DagsterRunStatus.QUEUED,
                    ],
                ),
                limit=100,
            )

            for run in in_progress_runs:
                if run.job_name == self.job_name:
                    return dg.SkipReason(f"Job already running: {run.run_id}")

            return dg.RunRequest(run_key=None)

        schedule = dg.ScheduleDefinition(
            name=f"{self.job_name}_schedule",
            job=job,
            cron_schedule=self.cron_schedule,
            execution_fn=schedule_fn,
        )

        return dg.Definitions(schedules=[schedule], jobs=[job])
```

### 3. Consolidated YAML (`defs/analytics/defs.yaml`)

Multiple component instances in one file using `---` separators:

```yaml
# dbt Analytics Component
type: dbt_automation_demo.defs.analytics.component.CustomDbtComponent
attributes:
  project:
    project_dir: "{{ project_root }}/analytics_dbt"
    profiles_dir: "{{ project_root }}/analytics_dbt"
---
# Hourly schedule with run-in-progress protection
# Uses dagster/materialization tag to programmatically select tables only
type: dbt_automation_demo.defs.components.safe_scheduled_job_component.SafeScheduledJobComponent
attributes:
  job_name: "hourly_dbt_tables_job"
  cron_schedule: "0 * * * *"
  asset_selection: "tag:dagster/materialization=table"  # Excludes views programmatically
  skip_reason_prefix: "Hourly dbt tables schedule"
```

## Declarative Automation vs Schedules

This demo includes both approaches:

| Approach | Mechanism | Use Case |
|----------|-----------|----------|
| **Declarative Automation** | `AutomationCondition` on assets | Reactive, condition-based triggering |
| **Schedules** | `ScheduleDefinition` | Explicit, time-based triggering |

**Declarative Automation** (used on the dbt assets):
- Evaluated by the `default_automation_condition_sensor`
- Triggers based on conditions (code changes, cron ticks, upstream updates)
- `~in_progress()` is a condition you add to prevent overlapping

**Schedules** (the `SafeScheduledJobComponent`):
- Traditional cron-based job triggers
- Requires manual run-in-progress checking in the `execution_fn`
- Useful when you want explicit control over job triggering

In most cases, **declarative automation alone is sufficient**. The schedule is included here to demonstrate the pattern for cases where you need schedule-based control.

## Running the Demo

```bash
# Navigate to the project
cd my-demos/dbt-automation-demo

# Validate definitions load correctly
uv run dg check defs

# List all assets, jobs, and schedules
uv run dg list defs

# Check asset automation conditions
uv run dg list defs --json | python -c "
import sys, json
data = json.load(sys.stdin)
for asset in data.get('assets', []):
    key = asset.get('key')
    auto = asset.get('automation_condition', 'None')
    print(f'{key}: {auto[:60]}...' if auto and len(auto) > 60 else f'{key}: {auto}')
"

# Start the development server
uv run dg dev
```

Then open http://localhost:3000 to view the Dagster UI.

## Testing the Automation

### Test 1: Code Version Change (Views)

1. Open `analytics_dbt/models/staging/stg_customers.sql`
2. Make a small change (add a comment or column)
3. In the Dagster UI, check the Automation tab
4. The `stg_customers` asset should show as a candidate for materialization

### Test 2: Upstream Update Propagation

1. Materialize `raw_customers` seed
2. Watch the automation sensor detect that `stg_customers` should refresh (due to `newly_updated`)

### Test 3: Cron-Based Automation

1. Change a mart model's cron to run soon (e.g., `on_cron("* * * * *")` for every minute)
2. Observe the automation sensor triggering the table refresh
3. Verify that `~in_progress()` prevents duplicate runs

## Customization Ideas

### Different Cron Schedules by Model Path

```python
def get_asset_spec(self, manifest, unique_id, project):
    base_spec = super().get_asset_spec(manifest, unique_id, project)
    node = self.get_resource_props(manifest, unique_id)
    fqn = node.get("fqn", [])

    if "hourly" in fqn:
        condition = dg.AutomationCondition.on_cron("0 * * * *") & ~dg.AutomationCondition.in_progress()
    elif "daily" in fqn:
        condition = dg.AutomationCondition.on_cron("0 6 * * *") & ~dg.AutomationCondition.in_progress()
    else:
        condition = dg.AutomationCondition.eager()

    return base_spec.replace_attributes(automation_condition=condition)
```

### Tag-Based Automation

```python
def get_asset_spec(self, manifest, unique_id, project):
    base_spec = super().get_asset_spec(manifest, unique_id, project)
    node = self.get_resource_props(manifest, unique_id)
    tags = node.get("tags", [])

    if "critical" in tags:
        # Critical assets: refresh every 15 minutes
        condition = dg.AutomationCondition.on_cron("*/15 * * * *") & ~dg.AutomationCondition.in_progress()
    elif "batch" in tags:
        # Batch assets: daily refresh
        condition = dg.AutomationCondition.on_cron("0 4 * * *") & ~dg.AutomationCondition.in_progress()
    else:
        condition = dg.AutomationCondition.eager()

    return base_spec.replace_attributes(automation_condition=condition)
```

## Common AutomationCondition Patterns

```python
# Eager: refresh when any upstream updates (default behavior)
dg.AutomationCondition.eager()

# Cron-based with overlap protection
dg.AutomationCondition.on_cron("0 6 * * *") & ~dg.AutomationCondition.in_progress()

# Only when explicitly requested (no automatic refresh)
None  # Set automation_condition to None

# Refresh when code changes
dg.AutomationCondition.code_version_changed()

# Refresh when upstream updates
dg.AutomationCondition.newly_updated()

# Refresh when missing
dg.AutomationCondition.on_missing()

# Combined: cron OR when upstream updates
dg.AutomationCondition.on_cron("0 6 * * *") | dg.AutomationCondition.newly_updated()

# Cron with dependency check: only if all deps updated since last cron
dg.AutomationCondition.on_cron("0 6 * * *")  # This is built into on_cron already
```

## Testing

The automation conditions are thoroughly tested with unit tests to verify the correct behavior for each asset type.

### Running Tests

```bash
# Run all automation condition tests
uv run pytest tests/test_automation_conditions.py -v

# Run a specific test class
uv run pytest tests/test_automation_conditions.py::TestViewAutomation -v

# Run with coverage
uv run pytest tests/test_automation_conditions.py --cov=dbt_automation_demo
```

### Test Summary

| Test Class | Tests | What It Verifies |
|------------|-------|------------------|
| `TestViewAutomation` | 3 | Views use `on_missing \| code_version_changed \| newly_updated` |
| `TestStagingTable2MinRefresh` | 4 | Tables with `refresh_2min` tag use 2-minute cron |
| `TestMartTableDailyCron` | 4 | Mart tables use daily cron with midnight lookback |
| `TestOtherNonViewAssets` | 3 | Other non-view assets use hourly cron |
| `TestMaterializationTagging` | 4 | Assets are tagged with `dagster/materialization` |
| `TestIgnoreSelection` | 3 | Cron conditions ignore view dependencies |

**Total: 23 tests**

### Test Strategy

The tests validate automation conditions at two levels:

1. **Condition Structure Tests** - Verify the correct `AutomationCondition` type is applied:
   ```python
   def test_view_has_code_version_or_newly_updated(self):
       """Views should trigger on code changes or upstream updates."""
       spec = self._get_spec_for_model("stg_customers")
       condition_str = str(spec.automation_condition)
       assert "CodeVersionChanged" in condition_str
       assert "NewlyUpdated" in condition_str
   ```

2. **Ignore Selection Tests** - Verify that cron-based conditions properly exclude view dependencies:
   ```python
   def test_2min_table_ignores_view_deps(self):
       """2-min refresh tables should ignore view dependencies."""
       spec = self._get_spec_for_model("stg_orders_enriched")
       condition_str = str(spec.automation_condition)
       assert "dagster/materialization" in condition_str
       assert "view" in condition_str
   ```

3. **Tag Application Tests** - Verify programmatic tagging works correctly:
   ```python
   def test_view_tagged_as_view(self):
       """Views should have dagster/materialization=view tag."""
       spec = self._get_spec_for_model("stg_customers")
       assert spec.tags.get("dagster/materialization") == "view"
   ```

### Key Testing Insights

- **Condition String Inspection**: Tests use `str(spec.automation_condition)` to inspect the condition tree
- **Asset Graph Loading**: Tests use `defs.resolve_asset_graph().get_all_asset_keys()` to validate asset creation
- **Model Selection**: Tests filter assets by checking if the model name is in the asset key path

### Test File Structure

```
tests/
└── test_automation_conditions.py    # 22 unit tests for automation conditions
```

The test file loads the actual Dagster definitions and inspects the generated `AssetSpec` objects, ensuring that the automation logic works correctly with real dbt project metadata.

## References

- [Dagster Declarative Automation](https://docs.dagster.io/concepts/automation/declarative-automation)
- [AutomationCondition API](https://docs.dagster.io/_apidocs/assets#dagster.AutomationCondition)
- [dbt Integration](https://docs.dagster.io/integrations/dbt)
- [Components Guide](https://docs.dagster.io/guides/build/components)

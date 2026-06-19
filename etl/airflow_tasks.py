"""Reusable Airflow task-group builder for the open-data ETL.

Import this from ANY DAG once the `etl` package is on sys.path:

    import sys
    sys.path.insert(0, "/opt/airflow/jobs/opendata_etl")   # the job folder
    from etl.airflow_tasks import build_etl_taskgroups

    with DAG(...) as dag:
        build_etl_taskgroups(groups=["th", "nyc"])   # list of origins; None = all

This module has NO import side effects (it does not create a DAG), so it is
safe to import into an existing DAG. `airflow` is imported lazily inside the
function so importing the package elsewhere doesn't require Airflow.
"""
from __future__ import annotations
from collections import defaultdict

from . import config
from .pipeline import run_dataset


def build_etl_taskgroups(groups: list[str] | None = None) -> dict:
    """Create one TaskGroup per origin (filtered by the `groups` list; None=all),
    each holding the dataset tasks. Call INSIDE an active `with DAG(...)` block.
    Returns {group: TaskGroup} so callers can wire dependencies.
    """
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

    wanted = set(groups) if groups else None
    by_group: dict[str, list[str]] = defaultdict(list)
    for src in config.load_registry():
        g = src.get("group", "default")
        if wanted is None or g in wanted:
            by_group[g].append(src["name"])

    made: dict = {}
    for group, names in by_group.items():
        with TaskGroup(group_id=group) as tg:
            for name in names:
                # drop redundant origin prefix inside the group (nyc.collisions_…)
                task_id = name[len(group) + 1:] if name.startswith(f"{group}_") else name
                PythonOperator(
                    task_id=task_id,
                    python_callable=run_dataset,
                    op_args=[name],
                )
        made[group] = tg
    return made

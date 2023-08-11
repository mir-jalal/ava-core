import json
import os
import string

import pandas as pd
import wta
from wta import EventLogIDs
from werkzeug.datastructures import FileStorage
from wta.main import run
import logging

from apps.metrics.models import EventLog, WaitingTimeResult, TraceResult, ActivityResult, Activity

from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration


# function to read the csv file
def read_event_log(file: FileStorage, log_path: str, log_ids: EventLogIDs) -> pd.DataFrame:
    # Read the event log
    # if tmp doesn't exist it will be created
    if not os.path.exists('tmp'):
        os.makedirs('tmp')

    file.save(f'tmp/{log_path}')
    event_log = pd.read_csv(f'tmp/{log_path}')
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    event_log[log_ids.resource].fillna("NOT_SET", inplace=True)
    event_log[log_ids.resource] = event_log[log_ids.resource].astype("string")
    return event_log


def store_event_log(file: FileStorage, event_log_id: int) -> str:
    if not os.path.exists('tmp'):
        os.makedirs('tmp')

    file.save(f'tmp/event_log_{event_log_id}.csv')
    return "ok"


def get_cohorts(event_log: EventLog) -> dict:
    event_log_df = pd.read_csv(f'tmp/event_log_{event_log.id}.csv')
    columns = event_log.get_columns()

    cohorts = {}

    for column in event_log_df.columns:
        if column not in columns:
            cohorts[column] = {
                str(value): len(event_log_df[event_log_df[column] == value]) for value in event_log_df[column].unique()
            }
    return cohorts


# enablement times function
def add_enablement_times(event_log: pd.DataFrame, log_ids: EventLogIDs):
    # Set up default configuration
    configuration = Configuration(
        log_ids=log_ids,  # Custom the column IDs with this parameter
        consider_start_times=True  # Consider real parallelism if the start times are available
    )
    # Instantiate desired concurrency oracle
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, configuration)
    # Add enablement times to the event log
    concurrency_oracle.add_enabled_times(event_log)


def get_activity_mapping(event_log: pd.DataFrame, log_ids: EventLog):
    characters = string.printable
    activities = set(event_log[log_ids.activity].unique())
    # Check if there are enough characters to map all activities
    if len(activities) > len(characters):
        raise RuntimeError(
            "Not enough characters ({}) to map all activities ({}) when clustering by activity sequence!".format(
                len(characters),
                len(activities)
            )
        )
    # Return activity-character mapping
    return {activity: characters[index] for index, activity in enumerate(activities)}


def cluster_traces(event_log: pd.DataFrame, log_ids: EventLog, filter_cohort: str):
    # Get the mapping from activity to character
    mapping = get_activity_mapping(event_log, log_ids)
    # Define mapping from sequence to case IDs
    clusters = {}
    # For each case, map the activities to character sequence
    for case_id, events in event_log.groupby([log_ids.case_id]):
        sorted_events = events.sort_values([log_ids.end_time, log_ids.start_time])
        activity_sequence = "".join([mapping[activity] for activity in sorted_events[log_ids.activity]])
        if activity_sequence in clusters:
            clusters[activity_sequence] += [case_id]
        else:
            clusters[activity_sequence] = [case_id]
    # Set cluster ID column for each cluster
    cluster_id = 0
    for cluster in clusters:
        event_log.loc[
            event_log[log_ids.case_id].isin(clusters[cluster]),
            'cluster_id'
        ] = cluster_id
        cluster_id += 1


def find_in_traces(traces: list, trace: list):
    for i, t in enumerate(traces):
        if t == trace:
            return i
    return -1


def get_activities_from_trace(unique_activities: dict, trace: list):
    activities = []
    for activity in trace:
        activities.append(unique_activities[activity].activity)
    return activities


def analysis_process_time(event_log: pd.DataFrame, default_log_ids: EventLog, filter_cohort: str, filter_value: str):
    filtered_event_log = get_filtered_event_log(event_log, filter_cohort, filter_value)
    cluster_traces(filtered_event_log, default_log_ids, filter_cohort)

    unique_traces = []
    unique_activities = {}

    all_traces = []
    activity_count = 0
    trace_count = 0

    for cluster_id, cluster_events in filtered_event_log.groupby('cluster_id'):
        for source_id, events in cluster_events.groupby(filter_cohort):
            for case_id, activities in events.groupby(default_log_ids.case_id):
                trace_count += 1
                current_trace = []
                is_first = True
                total_process_time = 0
                cycle_time_start = None
                cycle_time_end = None
                for index, row in activities.iterrows():
                    activity_count += 1
                    if is_first:
                        is_first = False
                        cycle_time_start = row[default_log_ids.start_time]
                    cycle_time_end = row[default_log_ids.end_time]
                    #
                    # if row[default_log_ids.activity] not in activity_map.keys():
                    #     activity_map[row[default_log_ids.activity]] = unique_activity_counter
                    #     activity_index_map[unique_activity_counter] = row[default_log_ids.activity]
                    #     current_trace.append(unique_activity_counter)
                    #
                    #     unique_activity_counter += 1
                    # else:
                    #     current_trace.append(activity_map[row[default_log_ids.activity]])

                    process_time = row[default_log_ids.end_time] - row[default_log_ids.start_time]
                    total_process_time = process_time if total_process_time == 0 else process_time + total_process_time

                    if row[default_log_ids.activity] not in unique_activities.keys():
                        unique_activities[row[default_log_ids.activity]] = \
                            ActivityResult(activity=Activity(row[default_log_ids.activity]), count=1,
                                           pt_total=process_time.total_seconds())
                    else:
                        unique_activities[row[default_log_ids.activity]].pt_total += process_time.total_seconds()
                        unique_activities[row[default_log_ids.activity]].count += 1

                    current_trace.append(row[default_log_ids.activity])

                current_trace_id = find_in_traces(all_traces, current_trace)

                if current_trace_id == -1:
                    unique_traces.append(
                        TraceResult(
                            ct_total=(cycle_time_end - cycle_time_start).total_seconds(),
                            pt_total=total_process_time.total_seconds(),
                            count=1,
                            activities=get_activities_from_trace(unique_activities, current_trace)))

                    all_traces.append(current_trace)
                else:
                    unique_traces[current_trace_id].count += 1
                    unique_traces[current_trace_id].ct_total += (cycle_time_end - cycle_time_start).total_seconds()
                    unique_traces[current_trace_id].pt_total += total_process_time.total_seconds()

    return list(unique_activities.values()), unique_traces


def get_filtered_event_log(event_log, filter_cohort, filter_value):
    if pd.api.types.is_integer_dtype(event_log[filter_cohort]):
        filter_value_list = [int(value) for value in filter_value.split(',')]
    elif pd.api.types.is_float_dtype(event_log[filter_cohort]):
        filter_value_list = [float(value) for value in filter_value.split(',')]
    else:
        filter_value_list = [value for value in filter_value.split(',')]
    return event_log[event_log[filter_cohort].isin(filter_value_list)]


def analysis_waiting_time(event_log: pd.DataFrame, event_log_id: EventLog, filter_cohort: str, filter_value: str):
    logs_ids = EventLogIDs(
        start_time=event_log_id.start_time,
        end_time=event_log_id.end_time,
        case=event_log_id.case_id,
        activity=event_log_id.activity,
        resource=event_log_id.resource,
    )

    add_enablement_times(event_log, logs_ids)

    filtered_event_log = get_filtered_event_log(event_log, filter_cohort, filter_value)
    # cluster_traces(filtered_event_log, event_log_id, filter_cohort)

    wt_analysis = run(log_path=None, log=filtered_event_log, log_ids=logs_ids, group_results=False, parallel_run=False)

    waiting_time_results = []

    for source_activity, activities in wt_analysis.groupby('source_activity'):
        for destination_activity, sub_activities in activities.groupby('destination_activity'):
            waiting_time_results.append(WaitingTimeResult(
                source_activity=source_activity,
                target_activity=destination_activity,
                count=len(sub_activities),
                wt_total=sub_activities['wt_total'].sum().total_seconds(),
                wt_contention=sub_activities['wt_contention'].sum().total_seconds(),
                wt_batching=sub_activities['wt_batching'].sum().total_seconds(),
                wt_prioritization=sub_activities['wt_prioritization'].sum().total_seconds(),
                wt_unavailability=sub_activities['wt_unavailability'].sum().total_seconds(),
                wt_extraneous=sub_activities['wt_extraneous'].sum().total_seconds(),
            ))

    return waiting_time_results


def get_waiting_time_result_by_activity(waiting_time_results: list, source_activity: str, target_activity: str):
    for waiting_time_result in waiting_time_results:
        if waiting_time_result["source_activity"] == source_activity and \
                waiting_time_result["target_activity"] == target_activity:
            return waiting_time_result
    return None


def generate_transition_difference_table_rows(waiting_time_results1, waiting_time_results2, process_time, cycle_time):
    rows = []
    count = 1
    for waiting_time1 in waiting_time_results1["waiting_times"]:
        source_activity = waiting_time1["source_activity"]
        target_activity = waiting_time1["target_activity"]
        waiting_time2 = get_waiting_time_result_by_activity(waiting_time_results2["waiting_times"], source_activity,
                                                            target_activity)

        if waiting_time2 is not None:
            waiting_time = (waiting_time1["wt_total"] + waiting_time2["wt_total"]) / 2
            rows.append({
                "cte_impact": round((cycle_time / process_time - (cycle_time - waiting_time) / process_time), 2),
                "number": count,
                "source_activity": source_activity,
                "target_activity": target_activity,
                "average_duration_first": waiting_time1["wt_total"] / waiting_time1["count"],
                "average_duration_second": waiting_time2["wt_total"] / waiting_time2["count"],
                "average_duration_difference": (waiting_time1["wt_total"] / waiting_time1["count"]) -
                                               (waiting_time2["wt_total"] / waiting_time2["count"]),
                "relative_frequency_first": waiting_time1["count"] / waiting_time_results1["total_count"],
                "relative_frequency_second": waiting_time2["count"] / waiting_time_results2["total_count"],
                "relative_frequency_difference": (waiting_time1["count"] / waiting_time_results1["total_count"]) -
                                                 (waiting_time2["count"] / waiting_time_results2["total_count"]),
            })
            count += 1
    return rows

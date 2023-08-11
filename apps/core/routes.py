import json

import pandas as pd
from flask import Blueprint, request, jsonify
from io import BytesIO

from werkzeug.datastructures import FileStorage

from apps.core.celery import do_analyze
from apps.metrics.models import db, EventLog, AnalyzeResult

from apps.metrics import EventLogIDs, read_event_log, analysis_waiting_time, \
    analysis_process_time, cluster_traces, store_event_log, get_cohorts, generate_transition_difference_table_rows

blue_print = Blueprint('core', __name__, url_prefix='/api/v1/core')


@blue_print.route('/waiting-time', methods=['POST'])
def calculate_waiting():
    file = request.files['event_log']
    filter_cohort = request.form['filter_cohort']
    filter_value = request.form['filter_value']
    default_log_ids = EventLogIDs()

    event_log = read_event_log(file, 'event_log.csv.gz', default_log_ids)

    return analysis_waiting_time(event_log, default_log_ids, filter_cohort, filter_value)


@blue_print.route('/process-time', methods=['POST'])
def calculate_process():
    file = request.files['event_log']
    filter_cohort = request.form['filter_cohort']
    filter_value = request.form['filter_value']
    default_log_ids = EventLogIDs()

    event_log = read_event_log(file, 'event_log.csv.gz', default_log_ids)
    cluster_traces(event_log, default_log_ids)

    return analysis_process_time(event_log, default_log_ids, filter_cohort, filter_value)


@blue_print.route('/analyze', methods=['POST', 'GET'])
def analyze():
    event_log_id = request.args['log_id']
    #event_log_ids = EventLog.query.filter_by(id=event_log_id).first()
    #event_log = pd.read_csv(f'tmp/event_log_{event_log_ids.id}.csv')
    filter_cohort = request.args['filter_cohort']
    filter_value1 = request.args['filter_value1']
    filter_value2 = request.args['filter_value2']

    analyze_result1 = AnalyzeResult(event_log_id, filter_cohort, filter_value1)
    analyze_result2 = AnalyzeResult(event_log_id, filter_cohort, filter_value2)
    db.session.add(analyze_result1)
    db.session.add(analyze_result2)
    db.session.commit()

    do_analyze.delay(analyze_result1.id, analyze_result2.id)

    return {
        "analyze_result1": analyze_result1.id,
        "analyze_result2": analyze_result2.id

    }


@blue_print.route('/upload', methods=['POST'])
def upload():
    file = request.get_data()
    file_data = BytesIO(file)

    start_time = request.args['start_time']
    end_time = request.args['end_time']
    resource = request.args['resource']
    activity = request.args['activity']
    case_id = request.args['case_id']

    event_log = EventLog(case_id, activity, start_time, end_time, resource)
    db.session.add(event_log)
    db.session.commit()

    return {
        'status': store_event_log(FileStorage(file_data), event_log.id),
        'id': event_log.id,
        'cohorts': get_cohorts(event_log)
    }


@blue_print.route('/results/<result_id>', methods=['GET'])
def get_results(result_id):
    analyze_result = AnalyzeResult.query.filter_by(id=result_id).first()
    return analyze_result.to_dict()


@blue_print.route('/results', methods=['GET'])
def get_two_results():
    result_id1 = request.args['result_id1']
    result_id2 = request.args['result_id2']
    analyze_result1 = AnalyzeResult.query.filter_by(id=result_id1).first().to_dict()
    analyze_result2 = AnalyzeResult.query.filter_by(id=result_id2).first().to_dict()
    process_time = (analyze_result1["trace_results"]["process_time"] + analyze_result2["trace_results"]["process_time"]) / 2
    cycle_time = (analyze_result1["trace_results"]["cycle_time"] + analyze_result2["trace_results"]["cycle_time"]) / 2
    transition_difference_table_rows = generate_transition_difference_table_rows(
        analyze_result1["waiting_time_results"],
        analyze_result2["waiting_time_results"],
        process_time, cycle_time)
    return {
        "result1": analyze_result1,
        "result2": analyze_result2,
        "transition_difference_table_rows": transition_difference_table_rows
    }

import pandas as pd

from apps.metrics import analysis_waiting_time, analysis_process_time
from celery import Celery, Task, shared_task
from flask import Flask

from logging import getLogger

from apps.metrics.models import AnalyzeResult, db


logger = getLogger(__name__)


def celery_init_app(app: Flask) -> Celery:
    class FlaskTask(Task):
        def run(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return super(FlaskTask, self).__call__(*args, **kwargs)
    celery_app = Celery(app.name)
    celery_app.config_from_object(app.config["CELERY"])
    celery_app.Task = FlaskTask
    celery_app.set_default()
    app.extensions['celery'] = celery_app
    return celery_app


@shared_task(name='apps.core.celery')
def do_analyze(analyze_result_id1: int, analyze_result_id2: int):
    do_one_analyze(analyze_result_id1)
    do_one_analyze(analyze_result_id2)


def do_one_analyze(analyze_result_id: int):
    logger.info(f"Task do_analyze started for analyze_result_id: {analyze_result_id}")

    analyze_result = AnalyzeResult.query.filter_by(id=analyze_result_id).first()
    event_log_meta = analyze_result.event_log

    event_log = pd.read_csv(f'tmp/event_log_{event_log_meta.id}.csv')
    event_log[event_log_meta.start_time] = pd.to_datetime(event_log[event_log_meta.start_time], utc=True)
    event_log[event_log_meta.end_time] = pd.to_datetime(event_log[event_log_meta.end_time], utc=True)
    event_log[event_log_meta.resource].fillna("NOT_SET", inplace=True)
    event_log[event_log_meta.resource] = event_log[event_log_meta.resource].astype("string")
    filter_cohort = analyze_result.cohort
    filter_value = analyze_result.cohort_values

    analyze_result.waiting_time_results = analysis_waiting_time(event_log, event_log_meta, filter_cohort, filter_value)
    analyze_result.activity_results, analyze_result.trace_results = \
        analysis_process_time(event_log, event_log_meta, filter_cohort, filter_value)

    db.session.commit()

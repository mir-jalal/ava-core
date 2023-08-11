from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

db = SQLAlchemy()


class WaitingTimeResult(db.Model):
    id = Column(Integer, primary_key=True)
    analyze_result_id = Column(Integer, ForeignKey('analyze_result.id'))
    source_activity = Column(String)
    target_activity = Column(String)
    count = Column(Integer)
    wt_total = Column(Integer)
    wt_contention = Column(Integer)
    wt_batching = Column(Integer)
    wt_prioritization = Column(Integer)
    wt_unavailability = Column(Integer)
    wt_extraneous = Column(Integer)

    analyze_result = relationship("AnalyzeResult", back_populates="waiting_time_results")

    def to_dict(self):
        return {
            'id': self.id,
            'source_activity': self.source_activity,
            'target_activity': self.target_activity,
            'count': self.count,
            'wt_total': self.wt_total,
            'wt_contention': self.wt_contention,
            'wt_batching': self.wt_batching,
            'wt_prioritization': self.wt_prioritization,
            'wt_unavailability': self.wt_unavailability,
            'wt_extraneous': self.wt_extraneous,
        }

    def __init__(self, source_activity, target_activity, count, wt_total, wt_contention, wt_batching, wt_prioritization,
                 wt_unavailability, wt_extraneous):
        self.source_activity = source_activity
        self.target_activity = target_activity
        self.count = count
        self.wt_total = wt_total
        self.wt_contention = wt_contention
        self.wt_batching = wt_batching
        self.wt_prioritization = wt_prioritization
        self.wt_unavailability = wt_unavailability
        self.wt_extraneous = wt_extraneous

    def __repr__(self):
        return f'<WaitingTimeResult {self.id}>'


class ActivityResult(db.Model):
    id = Column(Integer, primary_key=True)
    count = Column(Integer, nullable=False)
    pt_total = Column(Integer, nullable=False)
    analyze_result_id = Column(Integer, ForeignKey('analyze_result.id'))

    activity = relationship("Activity", back_populates="activity_result", uselist=False)
    analyze_result = relationship("AnalyzeResult", back_populates="activity_results")

    def to_dict(self):
        return {
            'id': self.id,
            'count': self.count,
            'pt_total': self.pt_total,
            'activity': self.activity.to_dict(),
        }

    def __init__(self, activity, count, pt_total):
        self.activity = activity
        self.count = count
        self.pt_total = pt_total

    def __repr__(self):
        return f'<ProcessTimeResult {self.id}>'


class Activity(db.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    activity_result_id = Column(Integer, ForeignKey('activity_result.id'))

    # trace_result_activities = relationship("TraceResultActivity", back_populates="activity")
    activity_result = relationship("ActivityResult", back_populates="activity", uselist=False)

    # trace_results = relationship(
    #     "TraceResult",
    #     secondary="trace_result_activity",
    #     order_by="TraceResultActivity.position",
    #     #collection_class=ordering_list('TraceResultActivity.position'),
    #     back_populates="activities"
    # )

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
        }

    def __init__(self, name):
        self.name = name


class TraceResult(db.Model):
    id = Column(Integer, primary_key=True)
    count = Column(Integer, nullable=False)
    ct_total = Column(Integer, nullable=False)
    pt_total = Column(Integer, nullable=False)
    analyze_result_id = Column(Integer, ForeignKey('analyze_result.id'))
    activities = Column(String)  # list of strings

    # activities = relationship(
    #     "Activity",
    #     secondary="trace_result_activity",
    #     order_by="TraceResultActivity.position",
    #     #collection_class=ordering_list('TraceResultActivity.position'),
    #     back_populates="trace_results"
    # )
    # trace_result_activities = relationship("TraceResultActivity", back_populates="trace_result")
    analyze_result = relationship("AnalyzeResult", back_populates="trace_results")

    def to_dict(self):
        return {
            'id': self.id,
            'count': self.count,
            'ct_total': self.ct_total,
            'pt_total': self.pt_total,
            # 'activities': [activity.to_dict() for activity in
            #                db.session.query(Activity).filter(Activity.name.in_(self.activities.split(','))).all()],
        }

    def __init__(self, ct_total, pt_total, count, activities):
        self.count = count
        self.ct_total = ct_total
        self.pt_total = pt_total
        self.activities = ','.join([activity.name for activity in activities])

    def __repr__(self):
        return f'<TraceResult {self.id}>'


def get_process_map(waiting_time_results, wt_total_count, activity_results, pt_total_count):
    process_map = [{'name': 'start', 'transitions': []}, {'name': 'end', 'transitions': []}]
    source_activities = []
    target_activities = []

    activity_indices = [activity_result.activity.name for activity_result in activity_results]

    for wt in waiting_time_results:
        source_activities.append(wt.source_activity)
        target_activities.append(wt.target_activity)
        if wt.source_activity not in [activity['name'] for activity in process_map]:
            activity_result = activity_results[activity_indices.index(wt.source_activity)]
            process_map.append({
                'name': wt.source_activity,
                'transitions': [{
                    'target': wt.target_activity, 'value': {
                        'average_duration': wt.wt_total / wt.count,
                        'total_duration': wt.wt_total,
                        'total_frequency': wt.count,
                        'relative_frequency': wt.count / wt_total_count
                    }
                }],
                'value': {
                    'average_duration': activity_result.pt_total / activity_result.count,
                    'total_duration': activity_result.pt_total,
                    'total_frequency': activity_result.count,
                    'relative_frequency': activity_result.count / pt_total_count
                }
            })
        else:
            process_map[[activity['name'] for activity in process_map].index(wt.source_activity)]['transitions'].\
                append({'target': wt.target_activity, 'value': {
                    'average_duration': wt.wt_total / wt.count,
                    'total_duration': wt.wt_total,
                    'total_frequency': wt.count,
                    'relative_frequency': wt.count / wt_total_count
            }})

    for activity in source_activities:
        if activity not in target_activities:
            process_map[0]['transitions'].append({'target': activity, 'value': ''})

    for activity in target_activities:
        if activity not in source_activities:
            activity_result = activity_results[activity_indices.index(activity)]
            process_map.append({
                'name': activity,
                'transitions': [{'target': 'end', 'value': ''}],
                'value': {
                    'average_duration': activity_result.pt_total / activity_result.count,
                    'total_duration': activity_result.pt_total,
                    'total_frequency': activity_result.count,
                    'relative_frequency': activity_result.count / pt_total_count
                }
            })

    return process_map


class AnalyzeResult(db.Model):
    id = Column(Integer, primary_key=True)
    event_log_id = Column(Integer, ForeignKey('event_log.id'))
    cohort = Column(String)
    cohort_values = Column(String)  # list of strings

    event_log = relationship("EventLog", back_populates="analyze_result")
    waiting_time_results = relationship("WaitingTimeResult")
    activity_results = relationship("ActivityResult")
    trace_results = relationship("TraceResult")

    def to_dict(self):
        total_waiting_time_count =sum([wt.count for wt in self.waiting_time_results])
        total_process_time_count = sum([ar.count for ar in self.activity_results])
        return {
            'id': self.id,
            'event_log_id': self.event_log_id,
            'cohort': self.cohort,
            'cohort_values': self.cohort_values,
            'waiting_time_results': {
                'waiting_time': sum([wt.wt_total for wt in self.waiting_time_results]),
                'wt_contention': sum([wt.wt_contention for wt in self.waiting_time_results]),
                'wt_batching': sum([wt.wt_batching for wt in self.waiting_time_results]),
                'wt_prioritization': sum([wt.wt_prioritization for wt in self.waiting_time_results]),
                'wt_unavailability': sum([wt.wt_unavailability for wt in self.waiting_time_results]),
                'wt_extraneous': sum([wt.wt_extraneous for wt in self.waiting_time_results]),
                'total_count': total_waiting_time_count,
                'waiting_times': [wt.to_dict() for wt in self.waiting_time_results],
            },
            'activity_results': {
                'process_time': sum([ar.pt_total for ar in self.activity_results]),
                'total_count': sum([ar.count for ar in self.activity_results]),
                'activities': [ar.to_dict() for ar in self.activity_results]
            },
            'trace_results': {
                'process_time': sum([tr.pt_total for tr in self.trace_results]),
                'cycle_time': sum([tr.ct_total for tr in self.trace_results]),
                'ct_efficiency': sum([tr.ct_total for tr in self.trace_results]) / sum(
                    [tr.pt_total for tr in self.trace_results]),
                'total_count': sum([tr.count for tr in self.trace_results]),
                'process_map': get_process_map(
                    self.waiting_time_results,
                    total_waiting_time_count,
                    self.activity_results,
                    total_process_time_count
                ),
                'traces': [tr.to_dict() for tr in self.trace_results]
            },
        }

    def __init__(self, event_log_id, cohort, cohort_value):
        self.event_log_id = event_log_id
        self.cohort = cohort
        self.cohort_values = cohort_value

    def __repr__(self):
        return f'<AnalyzeResults {self.event_log_id}>'


class EventLog(db.Model):
    id = Column(Integer, primary_key=True)
    case_id = Column(String(255))
    activity = Column(String(255))
    start_time = Column(String(255))
    end_time = Column(String(255))
    resource = Column(String(255))
    analyze_result = relationship("AnalyzeResult")

    def to_dict(self):
        return {
            'id': self.id,
            'case_id': self.case_id,
            'activity': self.activity,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'resource': self.resource,
        }

    def __init__(self, case_id, activity, start_time, end_time, resource):
        self.case_id = case_id
        self.activity = activity
        self.start_time = start_time
        self.end_time = end_time
        self.resource = resource

    def get_columns(self):
        return [self.case_id, self.activity, self.start_time, self.end_time, self.resource]

    def __repr__(self):
        return f'<EventLog {self.case_id}>'

# class TraceResultActivity(db.Model):
#     trace_result_id = Column(Integer, ForeignKey('trace_result.id'))
#     activity_id = Column(Integer, ForeignKey('activity.id'))
#     position = Column(Integer, primary_key=True)
#
#     # trace_result = relationship("TraceResult", back_populates="trace_result_activities")
#     # activity = relationship("Activity", back_populates="trace_result_activities")
#
#
# trace_result_activity = TraceResultActivity.__table__

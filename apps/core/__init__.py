import os

from flask import Flask
from flask_cors import CORS

from apps.core.celery import celery_init_app
from apps.core.routes import blue_print
from apps.metrics.models import db


def init_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    CORS(app)

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    app.config.from_mapping(
        CELERY=dict(
            broker_url='redis://localhost:6379',
            result_backend='redis://localhost:6379',
            task_ignore_result=True
        )
    )
    app.config.update(
        SQLALCHEMY_DATABASE_URI='sqlite:///mydatabase.db',
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/health')
    def health():
        return {'status': 'ok'}

    return app


core = init_app()
core.register_blueprint(blue_print)
celery_app = celery_init_app(core)

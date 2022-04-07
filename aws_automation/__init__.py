from flask import Flask
from flask_cors import CORS
from os import environ

app = Flask(__name__)

from aws_automation.backup import take_backup

CORS(app)
app.config["DEBUG"] = True
app.config['JSON_SORT_KEYS'] = False
app.config['JWT_SECRET_KEY'] = "secret"
app.config['JWT_TOKEN_LOCATION'] = ('headers', 'query_string')

app.register_blueprint(take_backup.bp)

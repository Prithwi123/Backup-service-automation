from os import environ
from bll import app
from flaskext.mysql import MySQL

db = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'prithwi'
app.config['MYSQL_DATABASE_PASSWORD'] = '123456'
app.config['MYSQL_DATABASE_DB'] = 'bll'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

MY_URL = environ.get('MY_HOST')
AML_URL = environ.get('AML_HOST')
PASSWORD = environ.get('STATIC_TOKEN')

aws_access_key_id = ''
aws_secret_access_key = ''
loc='us-east-2'
db.init_app(app)

import logging
import uuid
import os


LOGGER = logging.getLogger(__name__)

os.environ['SERVER_MAIN_PASSWORD'] = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'

DB_ID = str(uuid.uuid4()).replace('-', '')
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_USER = os.environ.get('DB_USER', 'admin')
DB_PASS = os.environ.get('DB_PASS', 'admin')
DB_SCHEMA = os.environ.get('DB_SCHEMA', f'test_{DB_ID}')

PATH_PRIVATE_KEY = os.environ.get('PATH_PRIVATE_KEY', str(os.path.join('tests', 'private_key.pem')))

os.environ['DB_HOST'] = DB_HOST
os.environ['DB_USER'] = DB_USER
os.environ['DB_PASS'] = DB_PASS
os.environ['DB_SCHEMA'] = DB_SCHEMA
os.environ['PATH_PRIVATE_KEY'] = PATH_PRIVATE_KEY

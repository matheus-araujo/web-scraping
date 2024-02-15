from utils import save_in_db
from sqlalchemy import create_engine
import configparser

config = configparser.ConfigParser()
config.read('database.ini')
db_params = config['postgresql']

db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(db_url, echo=True)

save_in_db()


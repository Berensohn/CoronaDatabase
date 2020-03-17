from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from create_db import load_json_file, add_rows_to_table, map_json_to_table, \
    get_sick_people_from_json

CORONA_DATA_JSON_FILE = 'corona_data.json'
CORONA_DB_ADDRESS = 'sqlite:///corona.db'

engine = create_engine(CORONA_DB_ADDRESS, echo=True)
Session = sessionmaker(bind=engine, autoflush=False)

if __name__ == '__main__':
    sick_people_json = load_json_file(CORONA_DATA_JSON_FILE)
    map_json_to_table(sick_people_json)
    sick_people = get_sick_people_from_json(sick_people_json)
    session = Session()
    add_rows_to_table(session, sick_people)
    session.close()

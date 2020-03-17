import json

from sqlalchemy import Column, Integer, Float, String, Table, MetaData
from sqlalchemy.orm import mapper

from sick_people import SickPeople

TABLE_NAME = 'sick_people'
PYTHON_TYPES_TO_SQLALCHEMY_TYPES = {float: Float, int: Integer, str: String(50)}


def convert_type_to_sqlalchemy_types(key):
    return PYTHON_TYPES_TO_SQLALCHEMY_TYPES[type(key)]


def load_json_file(file_path):
    try:
        with open(file_path, 'rb') as sick_people_json:
            sick_people = json.load(sick_people_json)
            return sick_people
    except FileNotFoundError as file_not_found_exception:
        raise (file_not_found_exception, f'{file_path} not found')


def get_sick_people_from_json(sick_people_json):
    sick_people = []
    for sick_person in sick_people_json:
        sick_people.append(SickPeople(**sick_person))
    return sick_people


def get_columns_from_json(sick_people_json):
    columns = []
    sick_person = sick_people_json[0]
    for key in sick_person.keys():
        primary_key_value = False
        if key == 'id':
            primary_key_value = True
        columns.append(Column(key, convert_type_to_sqlalchemy_types(sick_person[key]), primary_key=primary_key_value))
    return tuple(columns)


def map_json_to_table(sick_people_json):
    sick_people_information_columns = get_columns_from_json(sick_people_json)
    sick_people_table_mapper = Table(TABLE_NAME, MetaData(), *sick_people_information_columns)
    mapper(SickPeople, sick_people_table_mapper)


def add_rows_to_table(session, sick_people):
    session.add_all(sick_people)
    session.commit()
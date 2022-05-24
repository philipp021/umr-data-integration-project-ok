from email.policy import default
import MySQLdb
import pandas as pd
from typing import List, Dict, Tuple


# database information and credentials
db_host = 'localhost'
db_port = 3306
db_name = 'data_integration'
db_user = 'dataintegration_user'
db_password = 'DI_pass1234'

# location and name of input files
input_files = ['imdb_name_basics.tsv', 'imdb_title_basics.tsv', 'imdb_title_crew.tsv', 'imdb_title_ratings.tsv', 
               'netflix_titles.csv', 'amazon_prime_titles.csv']
input_file_path = './umr-data-integration-project-ok/0_datasets'

# data type and dataset specific settings
dataset_null_values = ['NaN', '\\N', '', 'nan']
data_type_translations = {int: 'INT', float: 'FLOAT', str: 'varchar'}
varchar_text_treshold = 10000  # if a column contains a string longer than this threshold, the data type text is used in mysql instead of varchar

def load_file(path: str) -> pd.DataFrame:
    """ Loads a csv/tsv file with header line. Chooses seperator based on file ending. """
    seperator = ','
    if path.endswith('.tsv'):
        seperator = '\t'
    df = pd.read_csv(path, sep=seperator, header=0)
    return df

def get_max_lengths(df: pd.DataFrame) -> List[int]:
    """ Computes the maximum length of the columns values as string for each column. """
    results = []
    for column in list(df):
        max_length = 0
        for value in df[column]:
            max_length = max(max_length, len(str(value)))
        results.append(max_length)
    return results

def get_table_types(df: pd.DataFrame) -> Dict[str, str]:
    """ Determines the type of the column to be either an integer, float or string. """
    possible_types = [int, float, str]
    column_types = {}
    for column in list(df):
        data_type_index = 0
        for value in df[column]:
            v = str(value)
            if v in dataset_null_values:
                continue
            while data_type_index < len(possible_types) - 1:
                try:
                    possible_types[data_type_index](v)  # try to convert value to current type
                except ValueError:
                    data_type_index += 1  # if conversion fails, try the next less restrictive type
                    continue
                break
            if data_type_index >= len(possible_types) - 1:
                break  # break if the last type in list is reached
        column_types[column] = data_type_translations[possible_types[data_type_index]]
    return column_types

def get_table_creation_statement(df: pd.DataFrame, table_name: str, pk_index: int = 0) -> Tuple[str, Dict[str, str]]:
    """ Generates the SQL statement to create a table for a given pandas DataFrame. 
        Also returns the data types used in the creation. """
    headers = list(df)
    column_types = get_table_types(df)
    value_lengths = get_max_lengths(df)
    statement = f'CREATE TABLE {table_name} ('
    for i, values in enumerate(zip(headers, value_lengths)):
        header, max_length = values
        if column_types[header] == data_type_translations[str]:
            # string ist either varchar or TEXT, based on length
            if max_length < varchar_text_treshold:
                statement += f'{header} varchar({max_length}),'
            else:
                statement += f'{header} TEXT,'
        else:
            # other types (int, float)
            statement += f'{header} {column_types[header]},'
        if i == pk_index:
            statement = statement[:-1]
            statement += ' NOT NULL,'
    statement += f'PRIMARY KEY ({headers[pk_index]}));'
    return statement, column_types

def get_row_creation_statement(table: str, row, headers: List[str], column_types: Dict[str, str]) -> str:
    """ Creates a SQL statement to insert a given row into the database table. """
    # remove null values
    row_headers = []
    values = [str(row[col]) for col in headers]
    for header, val in zip(headers, values):
        if val not in dataset_null_values:
            row_headers.append(header)
    values = [str(row[col]) for col in row_headers]
    values = map(lambda x: x.replace('\'', '\'\'') , values)  # sql does not like single quotes 

    statement = f'INSERT INTO {db_name}.{table} ('
    statement += ','.join(row_headers)
    statement += ') VALUES ('
    for h, v in zip(row_headers, values):
        if column_types[h] == data_type_translations[str]:
            statement += f'\'{str(v)}\','
        else:
            statement += f'{v},'
    return statement[:-1] + ');'

def add_file_to_db(file: str, cursor) -> None:
    """ Adds a file to the database. Creates a table and adds all the data. """
    df = load_file(f'{input_file_path}/{file}')
    assert(isinstance(cursor, MySQLdb.cursors.Cursor))

    # create table in database
    table_name = file.rsplit('.', 1)[0]  # remove file ending
    statement, column_types = get_table_creation_statement(df, table_name)
    cursor.execute(statement)
    print(f'\tcreated table {table_name}')

    # add data
    headers = list(df)
    rows_affected = 0
    for index, row in df.iterrows():
        statement = get_row_creation_statement(table_name, row, headers, column_types)
        rows_affected += cursor.execute(statement)
        if index % 10000 == 9999:
            print(f'\t{rows_affected} rows affected so far ...')
    print(f'\tadded {rows_affected} rows to table {table_name}')

def main():
    db = MySQLdb.connect(user=db_user, passwd=db_password, db=db_name, host=db_host, port=db_port)
    assert(isinstance(db, MySQLdb.connections.Connection))
    db_cursor = db.cursor()
    
    try:
        for f in input_files:
            print(f'Adding file {f} to database ...')
            add_file_to_db(f, db_cursor)
            db.commit()
            print()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, aborting!')
        print('Warning: The rows of the last table may not have been commited.')


if __name__ == '__main__':
    main()

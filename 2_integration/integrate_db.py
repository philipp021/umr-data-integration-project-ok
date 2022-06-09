#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Combines netflix_titles and amazon_prime_titles with the imdb rating.
    Stores the result in the database.
"""


import MySQLdb
import time
from typing import List, Dict, Tuple, Any


# database information and credentials
db_host = 'localhost'
db_port = 3306
db_name = 'data_integration'
db_user = 'dataintegration_user'
db_password = 'DI_pass1234'

# table information
movie_tables = ['netflix_titles', 'amazon_prime_titles']
movie_table_title_column = {'netflix_titles': 'title', 'amazon_prime_titles': 'title'}
movie_table_release_column = {'netflix_titles': 'release_year', 'amazon_prime_titles': 'release_year'}
movie_table_id_column = {'netflix_titles': 'show_id', 'amazon_prime_titles': 'show_id'}
hosted_on_values = {'netflix_titles': 'netflix', 'amazon_prime_titles': 'prime'}
integrated_table_name = 'rated_movies'
integrated_table_columns = {'title': 'varchar(300)', 'year': 'INT', 'imdb_id': 'varchar(12)', 'hosted_on': 'varchar(10)',
                            'host_id': 'varchar(6)', 'rating': 'FLOAT'}


def table_exists(db_cursor, name: str) -> bool:
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    try:
        db_cursor.execute(f'SELECT * FROM {db_name}.{name}')
        return True # if no exeption is thrown, the table already exists
    except MySQLdb.ProgrammingError:
        pass
    return False

def generate_movies(db_cursor, movie_table: str) -> Tuple[str, str, int]:
    """ A generator function that yields single entries of a movie table. """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    statement = 'SELECT '
    statement += movie_table_id_column[movie_table] + ','
    statement += movie_table_title_column[movie_table] + ','
    statement += movie_table_release_column[movie_table]
    statement += f' FROM {db_name}.{movie_table};'
    rows_affected = db_cursor.execute(statement)
    print(f'Queried {rows_affected} rows from table {movie_table}')
    while True:
        row = db_cursor.fetchone()
        if row is None:
            break
        yield row

def create_integrated_table(db_cursor) -> None:
    """ Creates the table where the integrated data can be stored. """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    # check if table already exists
    if table_exists(db_cursor, integrated_table_name):
        print(f'table {integrated_table_name} already exists')
        return

    s = f'CREATE TABLE {integrated_table_name} ('
    for name, datatype in integrated_table_columns.items():
        s += f'{name} {datatype}, '
    s = s[:-2] + ');'
    db_cursor.execute(s)
    print(f'new table {integrated_table_name} created')

def get_imdb_info_slow(db_cursor, title: str, year: int) -> Tuple[str, float] | None:
    """ Fetches IMDB id and rating for a movie from the database. This method performs a join for each query and thus is very slow. """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    title = title.replace("'", "''")
    statement = 'SELECT imdb_title_basics.tconst, imdb_title_ratings.averageRating '
    statement += 'FROM imdb_title_basics INNER JOIN imdb_title_ratings ON '
    statement += 'imdb_title_basics.tconst=imdb_title_ratings.tconst '
    statement += f'WHERE imdb_title_basics.startYear={year} AND ('
    statement += f'imdb_title_basics.primaryTitle=\'{title}\' OR '
    statement += f'imdb_title_basics.originalTitle=\'{title}\');'
    db_cursor.execute(statement)
    return db_cursor.fetchone()

def get_imdb_info_from_temp(db_cursor, temp_table: str, title: str, year: int) -> Tuple[str, float] | None:
    """ Fetches IMDB id and rating for a movie from the database. This method uses a temporary table. """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    title = title.replace("'", "''")
    statement = f'SELECT tconst, rating FROM {db_name}.{temp_table} '
    statement += f'WHERE year={year} AND ('
    statement += f'primaryTitle=\'{title}\' OR '
    statement += f'originalTitle=\'{title}\');'
    db_cursor.execute(statement)
    return db_cursor.fetchone()

def create_temporary_rating_table(db_cursor, temp_name: str) -> int:
    """ Creates a temporary table for IMDB data containing only id, title (primary+original), year and rating.
        Returns the row count of the new table.
    """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    # create table
    if table_exists(db_cursor, temp_name):
        print(f'temporary table {temp_name} already exists')
        return
    statement = f'CREATE TABLE {db_name}.{temp_name} (tconst varchar(12), primaryTitle varchar(300), originalTitle varchar(300), year INT, rating FLOAT);'
    db_cursor.execute(statement)

    # add data
    statement = f'INSERT INTO {temp_name} SELECT imdb_title_basics.tconst, imdb_title_basics.primaryTitle, '
    statement += 'imdb_title_basics.originalTitle, imdb_title_basics.startYear, imdb_title_ratings.averageRating '
    statement += 'FROM imdb_title_basics INNER JOIN imdb_title_ratings ON imdb_title_basics.tconst=imdb_title_ratings.tconst;'
    rows_affected = db_cursor.execute(statement)
    print(f'created temporary rating table containing {rows_affected} rows')
    return rows_affected

def insert_rating(db_cursor, title: str, year: int, table: str, host_id: str, imdb_id: str, rating: float) -> None:
    """ Inserts a row with given values in the integrated table. """
    assert(isinstance(db_cursor, MySQLdb.cursors.Cursor))
    host = hosted_on_values[table]
    title = title.replace("'", "''")
    s = f'INSERT INTO {db_name}.{integrated_table_name} VALUES ('
    s += f"'{title}', {year}, '{imdb_id}', '{host}', '{host_id}', {rating});"
    db_cursor.execute(s)

def main() -> None:
    # connect to database and create cursors
    db = MySQLdb.connect(user=db_user, passwd=db_password, db=db_name, host=db_host, port=db_port)
    assert(isinstance(db, MySQLdb.connections.Connection))
    movie_query_cursor = db.cursor()
    imdb_query_cursor = db.cursor()
    insert_cursor = db.cursor()

    # create result and temporary table
    create_integrated_table(insert_cursor)
    db.commit()
    temp_name = 'temp_imdb_ratings'
    create_temporary_rating_table(imdb_query_cursor, temp_name)
    db.commit()

    try:
        for table in movie_tables:
            count = 0
            for host_id, title, year in generate_movies(movie_query_cursor, table):
                imdb = get_imdb_info_from_temp(imdb_query_cursor, temp_name, title, year)
                if imdb is None:
                    continue
                imdb_id, rating = imdb
                insert_rating(insert_cursor, title, year, table, host_id, imdb_id, rating)
                count += 1
                if count % 100 == 0:
                    print(f'{count} enties processed')
            db.commit()
    except KeyboardInterrupt:
        print('Interrupted. Changes have not been committed')
    
    movie_query_cursor.close()
    imdb_query_cursor.close()
    insert_cursor.close()

    # delete temporary table
    #insert_cursor.execute(f'DROP TABLE {db_name}.{temp_name};')
    #db.commit()


if __name__ == '__main__':
    main()

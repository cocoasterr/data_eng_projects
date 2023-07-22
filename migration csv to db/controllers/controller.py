from sqlalchemy import create_engine
import pandas as pd


def connect_db(url):
    """
    just connecting DB URL to sqlalchemy
    """
    return create_engine(url).connect()


def exec(conn, query):
    """General Exec using query SQL

    Args:
        conn : connection from engine sql
        query (str): query sql
    """
    conn.exec_driver_sql(query)
    conn.commit()


def create_db(db_name, url):
    """create database session

    Args:
        db_name (str): name of database
        url (str): url database

    Returns:
        _type_: _description_
    """
    try:
        conn = connect_db(url)
        create_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        exec(conn, create_query)
        conn.close()
        return "create db success!"
    except Exception as e:
        return f"Error \n{e}"


def migration_csv_to_db(db_name, url, tbl_name, df_file, is_init: bool = False):
    """_summary_

    Args:
        db_name (str): name of database
        url (str): url from database
        tbl_name (str): name of table database
        df_file (DataFrame): Data Frame from file
        is_init (bool, optional): if initialize create table select True. Defaults to False.

    Returns:
        _type_: _description_
    """
    try:
        conn = connect_db(url + db_name)
        df_file.to_sql(tbl_name, con=conn, if_exists="append", index=False)
        pd.read_sql_query
        if is_init:
            query_add_id = (
                f"ALTER TABLE {tbl_name} ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;"
            )
            exec(conn=conn, query=query_add_id)
        conn.close()
        return f"{tbl_name} migration success!"
    except Exception as e:
        return f"Error \n{e}"

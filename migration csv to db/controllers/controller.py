from sqlalchemy import create_engine
import pandas as pd


def connect_db(url):
    """Connecting database using sqlalchemy

    Args:
        url (str): DB Url

    Returns:
        engine: connection db transactions
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


def create_db(db_name, url, db_select: str = "mysql"):
    """create database session

    Args:
        db_name (str): name of database
        url (str): url database

    Returns:
        _type_: _description_
    """
    conn = connect_db(url)
    db = ""
    if "mysql" in db_select.lower():
        db = "IF NOT EXISTS"
        create_query = f"CREATE DATABASE {db} {db_name};"
        exec(conn, create_query)
        conn.close()
        return "create db success!"
    else:
        pass


def migration_csv_to_db(
    db_name, url, tbl_name, df_file, db_select: str = "mysql", is_init: bool = False
):
    """Migration csv file to database

    Args:
        db_name (str): name of database
        url (str): url from database
        tbl_name (str): name of table database
        df_file (DataFrame): Data Frame from file
        is_init (bool, optional): if initialize create table select True. Defaults to False.

    Returns:
        str: status migration
    """
    pd.read_sql_query
    if db_select.lower() == "mysql":
        conn = connect_db(url + db_name)
        type_sql = f"INT AUTO_INCREMENT "
    elif db_select.lower() == "postgres":
        conn = connect_db(url)
        type_sql = f"Serial"
    df_file.to_sql(tbl_name, con=conn, if_exists="append", index=False)
    query_add_id = f"ALTER TABLE {tbl_name} ADD COLUMN id {type_sql} PRIMARY KEY;"
    if is_init:
        exec(conn=conn, query=query_add_id)
    conn.close()
    return f"{tbl_name} migration success!"

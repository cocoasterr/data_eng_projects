import pandas as pd
from sqlalchemy import create_engine
import os


def connect_db(url):
    return create_engine(url).connect()


def exec(conn, query):
    conn.exec_driver_sql(query)
    conn.commit()


def create_db(db_name, url):
    try:
        conn = connect_db(url)
        create_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        exec(conn, create_query)
        conn.close()
        return "create db success!"
    except Exception as e:
        return f"Error \n{e}"


def migration_csv_to_db(db_name, url, tbl_name, df_file, is_init: bool = False):
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


mysql_url = os.getenv("MYSQL_DB_URL")
db_name = "first_project"
tbl_names = ["account_data", "account_country", "account_series"]

# create session
db_create = create_db(db_name, mysql_url)
print(db_create)


# migration to db
cur = os.getcwd()
account_data = pd.read_csv(f"{cur}/dataset/Wealth-AccountData.csv")
account_country = pd.read_csv(f"{cur}/dataset/Wealth-AccountsCountry.csv")[
    ["Code", "Short Name", "Region"]
]
account_series = pd.read_csv(f"{cur}/dataset/Wealth-AccountSeries.csv").drop(
    columns=["Previous Indicator Code", "Previous Indicator Name"], axis=1
)
df_files = [account_data, account_country, account_series]

for tbl_name, df_file in zip(tbl_names, df_files):
    migration = migration_csv_to_db(
        db_name, mysql_url, tbl_name, df_file, is_init=False
    )
    print(migration)

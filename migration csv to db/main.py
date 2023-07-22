import pandas as pd
import os
from controllers.controller import *

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

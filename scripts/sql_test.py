import pandas as pd
from sqlalchemy import create_engine
import yaml

# Load config
with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)
db_cfg = cfg["mysql_db"]
conn_str = db_cfg['url']
engine = create_engine(conn_str)

#
df_existing = pd.read_sql("SELECT * FROM `KPI`", engine)
print(df_existing)

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            CONSTRAINT_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = 'sql12796373' AND TABLE_NAME = 'Dữ liệu bán hàng';
    """))

    for row in result:
        print(row)

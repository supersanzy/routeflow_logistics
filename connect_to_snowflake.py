import os
import snowflake.connector as sf
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv('SF_USER')
ACCOUNT = os.getenv('SF_ACCOUNT')
PASSWORD = os.getenv('SF_PASSWORD')
WAREHOUSE = os.getenv('SF_WAREHOUSE')
DATABASE = os.getenv('SF_DATABASE')
SCHEMA = os.getenv('SF_SCHEMA')

stage_query = "PUT file:///home/supersanzy_de/macro_logistics_delivery_rate/routeflow_shipments_silver_layer.parquet @routeflow_stage;"
# quarantine_query = "PUT file:///home/supersanzy_de/macro_logistics_delivery_rate/routeflow_shipments_quarantine_data.parquet @routeflow_quarantine;"

try:
    conn = sf.connect(
        user=USER,
        account=ACCOUNT,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

    print("Snowflake Connection Successfull")

    conn.cursor().execute(stage_query)

except Exception as error:
    print(f"Snowflake unable to connect: {error}")
    raise

finally:
    conn.close()


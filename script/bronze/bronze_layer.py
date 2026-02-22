from pyspark.sql.functions import count
from bronze_ingestion_config import INGESTION_CONFIG

def main():
    for source in INGESTION_CONFIG:
      try:
        print(f"Ingesting {source['source']} into bronze.{source['table_name']}")

        df = spark.read.format(source["format"])\
        .options(header = source["header"], inferSchema = source["inferSchema"])\
        .load(source["path"])

        if df.limit(1).count() == 0:
          raise ValueError(f"No valid records for {source['table_name']}")

        df.write.mode("append")\
        .format("delta")\
        .saveAsTable(f"bronze.{source['table_name']}")

      except Exception as e:
        print(f"bronze.{source['table_name']} pipeline failed")
        raise e

if __name__ == "__main__":
    main()
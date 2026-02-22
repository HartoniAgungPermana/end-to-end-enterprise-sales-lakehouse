from pyspark.sql.functions import trim, when, col, upper
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Read the table
        df = spark.read.table("bronze.erp_px_cat_g1v2")

        # Trim all StringType Column
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        
        # Normalize flag value in MAINTENANCE into boolean
        df = df.withColumn(
            "MAINTENANCE",
            when(upper(col("MAINTENANCE")) == "YES", True)
            .when(upper(col("MAINTENANCE")) == "NO", False)
            .otherwise(None)
            )
    
        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for erp_px_cat_g1v2")

        # write the data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.erp_px_cat_g1v2")
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.erp_px_cat_g1v2 pipeline is failed.")
        raise e

if __name__ == "__main__":
    main(spark)
    
    
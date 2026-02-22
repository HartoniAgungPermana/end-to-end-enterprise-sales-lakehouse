from pyspark.sql.functions import regexp_replace, when, col, trim
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Read the table
        df = spark.read.table("bronze.erp_loc_a101")

        # Trim all str column
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))

        # Clean unwanted characters in CID column
        df = df.withColumn("CID", regexp_replace(col("CID"), "-", ""))

        # Standardize incostsitent data in CNTRY column
        df = df.withColumn(
            "CNTRY",
            when(col("CNTRY").isin("US", "USA"), "United States")
            .when(col("CNTRY") == "DE", "Germany")
            .when((col("CNTRY").isNull()) | (col("CNTRY") == ""), "N/A")
            .otherwise(col("CNTRY"))
            )
        
        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for erp_loc_a101")

        # write the data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.erp_loc_a101")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.erp_loc_a101 pipeline is failed.")
        raise e

if __name__ == "__main__":
    main(spark)
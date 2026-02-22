from pyspark.sql.functions import trim, col, length, when, substring, current_date
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Read the table
        df = spark.read.table("bronze.erp_cust_az12")

        # Trim all StringType columns
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        
        # Clean extra characters in CID column
        df = df.withColumn(
            "CID",
            when(col("CID").startswith("NAS"), substring(col("CID"), 4, length(col("CID")) - 3))
            .otherwise(col("CID"))
            )
        
        # Clean the irratiaonal date data
        df = df.withColumn(
            "BDATE",
            when(col("BDATE") > current_date(), None)
            .when(col("BDATE") < ("1926-02-10"), None)
            .otherwise(col("BDATE"))
            )
        
        # Standardize the inconsistencies of GEN column
        df = df.withColumn(
            "GEN",
            when((col("GEN") == "") | (col("GEN").isNull()), "N/A")
            .when(col("GEN") == "M", "Male")
            .when(col("GEN") == "F", "Female")
            .otherwise(col("GEN"))
            )
        
        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for erp_cust_az12")

        # write data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.erp_cust_az12")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.erp_cust_az12 pipeline is failed")
        raise e

if __name__ == "__main__":
    main(spark)
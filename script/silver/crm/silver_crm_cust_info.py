from pyspark.sql.functions import col, count, trim, when, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        df = spark.table("bronze.crm_cust_info")

        # Drop null value on PK
        df = df.dropna(subset=["cst_id"]) 

        # Clean historical duplicate data
        window = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

        df = (
            df.withColumn("rn", row_number().over(window))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # Trim all Stringtype coloumn
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        
        # Standardize cst_marital_status and cst_gndr
        df = (df.
            withColumn("cst_marital_status", when(col("cst_marital_status") == "M", "Married")
                        .when(col("cst_marital_status") == "S", "Single")
                        .otherwise("N/A"))
            .withColumn("cst_gndr", when(col("cst_gndr") == "M", "Male")
                        .when(col("cst_gndr") == "F", "Female")
                        .otherwise("N/A"))
            )
        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for crm_cust_info")

        # write data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.crm_cust_info")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.crm_cust_info pipeline is failed.")
        raise e

if __name__ == "__main__":
    main(spark)
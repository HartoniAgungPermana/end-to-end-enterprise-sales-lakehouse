from pyspark.sql.functions import trim, when, length, to_date, col, abs
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Read the table
        df = spark.read.table("bronze.crm_sales_details")

        # Trim all StringType column
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))
        
        # Clean the date data
        df = (df.withColumn("sls_order_dt",
                when((col("sls_order_dt").isNull()) | (length(col("sls_order_dt")) != 8), None)
                .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")))
        .withColumn("sls_ship_dt",
            when((col("sls_ship_dt").isNull()) | (length(col("sls_ship_dt")) != 8), None)
            .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))
        )
        .withColumn("sls_due_dt",
            when((col("sls_due_dt").isNull()) | (length(col("sls_due_dt")) != 8), None)
            .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")))
        )
        
        # Clean inconsistent sales, quantity, and price values from source system
        df = df.withColumn(
                "sls_sales",
                when(col("sls_sales") <= 0, abs(col("sls_quantity") * col("sls_price")))
                .when(col("sls_sales").isNull(), abs(col("sls_quantity") * col("sls_price")))
                .when(col("sls_sales") != col("sls_quantity") * col("sls_price"), abs(col("sls_quantity") * col("sls_price"))).otherwise(col("sls_sales"))
            ).withColumn(
                "sls_price",
                when((col("sls_quantity").isNull()) | (col("sls_quantity") == 0),None)
                .when(
                    (col("sls_price") <= 0) | col("sls_price").isNull()
                    | (col("sls_price") != col("sls_sales") / col("sls_quantity")),
                    abs(col("sls_sales") / col("sls_quantity"))
                ).otherwise(col("sls_price"))
            )
        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for crm_sales_details")

        # Write the data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.crm_sales_details")
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.sales_details pipeline is failed.")
        raise e

if __name__ == "__main__":
    main(spark)
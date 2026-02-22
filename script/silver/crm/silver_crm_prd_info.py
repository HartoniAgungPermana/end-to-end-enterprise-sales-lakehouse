from pyspark.sql.functions import col, trim, substring, regexp_replace, when, length, lead
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Read the table
        df = spark.read.table("bronze.crm_prd_info")

        # Trim all string column
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df =  df.withColumn(field.name, trim(col(field.name)))

        # Extract prd_cat_id from prd_key
        df = (df
            .withColumn("prd_cat_id", substring(col("prd_key"), 1, 5))
            .withColumn("prd_cat_id", regexp_replace(col("prd_cat_id"), "-", "_"))
            .withColumn("prd_key", substring(col("prd_key"), 7, length(col("prd_key")) - 6))
        )

        # Handling null values on prd_cost
        df = df.fillna(0, subset = ["prd_cost"])

        # Standardize prd_line
        df = df.withColumn( "prd_line",
            when(col("prd_line") == "R", "Road")
            .when(col("prd_line") == "M", "Mountain")
            .when(col("prd_line") == "T", "Touring")
            .when(col("prd_line") == "S", "Other Sales")
            .otherwise("N/A")
            )

        # Clean the date data, End date is next product version start date
        window = Window.partitionBy("prd_key").orderBy(col("prd_start_dt").asc())

        df = df.withColumn("prd_end_dt",lead("prd_start_dt").over(window))

        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for crm_prd_info")

        # Write the data
        df.write.mode("overwrite").format("delta").saveAsTable("silver.crm_prd_info")

    except AnalysisException as ae:
        print("Table reference error.")
        raise ae

    except Exception as e:
        print("silver.crm_prd_info pipeline is failed.")
        
if __name__ == "__main__":
    main(spark)
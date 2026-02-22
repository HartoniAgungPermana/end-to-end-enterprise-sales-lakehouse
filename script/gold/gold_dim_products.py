from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Write SQL query
        query = '''
                SELECT
                    ROW_NUMBER() OVER (ORDER BY pi.prd_id ASC) AS product_key,
                    pi.prd_id AS product_id,
                    pi.prd_key AS product_number,
                    pi.prd_nm AS product_name,
                    pi.prd_cat_id AS category_id,
                    pc.CAT AS category,
                    pc.SUBCAT AS subcategory,
                    pi.prd_cost AS product_cost,
                    pc.MAINTENANCE AS maintenance_flag,
                    pi.prd_line AS product_line,
                    pi.prd_start_dt AS start_date
                FROM silver.crm_prd_info AS pi
                LEFT JOIN silver.erp_px_cat_g1v2 AS pc
                ON pi.prd_cat_id = pc.ID  
        '''

        # Assign into dataframe variable
        df = spark.sql(query)

        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for dim_products")

        # write data
        df.write.mode("overwrite").format("delta").saveAsTable("gold.dim_products")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae
    
    except Exception as e:
        print("gold.dim_products pipeline failed.")
        raise e

if __name__ == "__main__":
    main(spark)
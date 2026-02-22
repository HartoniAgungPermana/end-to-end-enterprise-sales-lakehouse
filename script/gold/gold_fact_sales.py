from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Write SQL query
        query = '''
                SELECT
                    csd.sls_ord_num AS order_number,
                    dc.customer_key AS customer_key,
                    dp.product_key AS product_key,
                    csd.sls_order_dt AS order_date,
                    csd.sls_ship_dt AS ship_date,
                    csd.sls_due_dt AS due_date,
                    csd.sls_sales AS sales,
                    csd.sls_quantity AS quantity,
                    csd.sls_price AS price
                FROM silver.crm_sales_details AS csd
                LEFT JOIN gold.dim_products AS dp
                ON csd.sls_prd_key = dp.product_number
                LEFT JOIN gold.dim_customers AS dc
                ON csd.sls_cust_id = dc.customer_id
        '''

        # Assign into dataframe variable
        df = spark.sql(query)

        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for fact_sales")

        # write data
        df.write.mode("overwrite").format("delta").saveAsTable("gold.fact_sales")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae
    
    except Exception as e:
        print("gold.fact_sales pipeline failed.")
        raise e

if __name__ == "__main__":
    main(spark)
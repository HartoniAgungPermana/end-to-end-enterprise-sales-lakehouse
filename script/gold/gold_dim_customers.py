from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

def main(spark):
    try:
        # Write SQL query
        query = '''
                SELECT
                    ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC) AS customer_key,
                    ci.cst_id AS customer_id,
                    ci.cst_key AS customer_number,
                    ci.cst_firstname AS first_name,
                    ci.cst_lastname AS last_name,
                    ci.cst_marital_status AS marital_status,
                    CASE
                        WHEN ci.cst_gndr IN ('Male', 'Female') THEN ci.cst_gndr
                        WHEN ci.cst_gndr = 'N/A' AND ca.GEN IS NOT NULL AND ca.GEN != 'N/A' THEN ca.GEN
                        ELSE ci.cst_gndr
                    END AS gender,
                    ca.BDATE AS birth_date,
                    la.CNTRY AS country
                FROM silver.crm_cust_info as ci
                LEFT JOIN silver.erp_cust_az12 as ca
                ON ci.cst_key = ca.CID
                LEFT JOIN silver.erp_loc_a101 as la
                ON ci.cst_key = la.CID
        '''

        # Assign into dataframe variable
        df = spark.sql(query)

        # Guardrail
        if df.limit(1).count() == 0:
            raise ValueError("No valid records for dim_customers")

        # write data
        df.write.mode("overwrite").format("delta").saveAsTable("gold.dim_customers")
    
    except AnalysisException as ae:
        print("Table reference error.")
        raise ae
    
    except Exception as e:
        print("gold.dim_customers pipeline failed.")
        raise e

if __name__ == "__main__":
    main(spark)
from crm.silver_crm_cust_info import main as crm_cust_info
from crm.silver_crm_prd_info import main as crm_prd_info
from crm.silver_crm_sales_details import main as crm_sales_details
from erp.silver_erp_cust_az12 import main as erp_cust_az12
from erp.silver_erp_loc_a101 import main as erp_loc_a101
from erp.silver_erp_px_cat_g1v2 import main as erp_px_cat_g1v2

def run_step(step_name, funct, spark):
    print(f"Starting: {step_name}")
    try:
        funct(spark)
        print(f"{step_name} is completed")
    except Exception as e:
        print(f"{step_name} is failed")
        raise e

def main(spark):
    print("Starting Silver Layer Orchestration")

    run_step("crm_cust_info", crm_cust_info, spark)
    run_step("crm_prd_info", crm_prd_info, spark)
    run_step("crm_sales_details", crm_sales_details, spark)
    run_step("erp_cust_az12", erp_cust_az12, spark)
    run_step("erp_loc_a101", erp_loc_a101, spark)
    run_step("erp_px_cat_g1v2", erp_px_cat_g1v2, spark)

    print("Silver Layer Load Has Been Completed Succesfully")

if __name__ == "__main__":
    main(spark)

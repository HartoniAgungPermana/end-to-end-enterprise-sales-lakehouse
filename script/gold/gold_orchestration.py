from gold_dim_customers import main as dim_customers
from gold_dim_products import main as dim_products
from gold_fact_sales import main as fact_sales

def run_step(step_name, funct, spark):
    print(f"Starting: {step_name}")
    try:
        funct(spark)
        print(f"{step_name} is completed")
    except Exception as e:
        print(f"{step_name} is failed")
        raise e

def main(spark):
    print("Starting Gold Layer Orchestration")

    run_step("dim_customers", dim_customers, spark)
    run_step("dim_products", dim_products, spark)
    run_step("fact_sales", fact_sales, spark)

    print("Gold Layer Load Has Been Completed Succesfully")

if __name__ == "__main__":
    main(spark)
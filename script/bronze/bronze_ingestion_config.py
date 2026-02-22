INGESTION_CONFIG = [
    {
        "source" : "crm",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/cust_info.csv",
        "table_name" : "crm_cust_info",
        "header" : True,
        "inferSchema" : True
    },
    {
        "source" : "crm",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/prd_info.csv",
        "table_name" : "crm_prd_info",
        "header" : True,
        "inferSchema" : True
    },
    {
        "source" : "crm",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/sales_details.csv",
        "table_name" : "crm_sales_details",
        "header" : True,
        "inferSchema" : True
    },
    {
        "source" : "erp",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/CUST_AZ12.csv",
        "table_name" : "erp_cust_az12",
        "header" : True,
        "inferSchema" : True
    },
    {
        "source" : "erp",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/LOC_A101.csv",
        "table_name" : "erp_loc_a101",
        "header" : True,
        "inferSchema" : True
    },
    {
        "source" : "erp",
        "format" : "csv",
        "path" : "/Volumes/workspace/bronze/data_sources/PX_CAT_G1V2.csv",
        "table_name" : "erp_px_cat_g1v2",
        "header" : True,
        "inferSchema" : True
    }
]
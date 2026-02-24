# 配置文件
# 数据文件路径配置

# TPC-DS 数据文件路径
DATA_PATH = "C:\\tpcds-data"

# 项目根目录
PROJECT_ROOT = "C:\\Users\\o2309\\PycharmProjects\\PythonProject1"

# 视图定义文件路径
VIEW_DDL_FILE = f"{PROJECT_ROOT}\\0view.sql"

# 数据文件路径配置
DATA_FILES = {
    "call_center": f"{DATA_PATH}\\call_center.dat",
    "customer_address": f"{DATA_PATH}\\customer_address.dat",
    "customer_demographics": f"{DATA_PATH}\\customer_demographics.dat",
    "date_dim": f"{DATA_PATH}\\date_dim.dat",
    "warehouse": f"{DATA_PATH}\\warehouse.dat",
    "ship_mode": f"{DATA_PATH}\\ship_mode.dat",
    "time_dim": f"{DATA_PATH}\\time_dim.dat",
    "reason": f"{DATA_PATH}\\reason.dat",
    "income_band": f"{DATA_PATH}\\income_band.dat",
    "item": f"{DATA_PATH}\\item.dat",
    "store": f"{DATA_PATH}\\store.dat",
    "customer": f"{DATA_PATH}\\customer.dat",
    "web_site": f"{DATA_PATH}\\web_site.dat",
    "store_returns": f"{DATA_PATH}\\store_returns.dat",
    "household_demographics": f"{DATA_PATH}\\household_demographics.dat",
    "web_page": f"{DATA_PATH}\\web_page.dat",
    "promotion": f"{DATA_PATH}\\promotion.dat",
    "catalog_page": f"{DATA_PATH}\\catalog_page.dat",
    "inventory": f"{DATA_PATH}\\inventory.dat",
    "catalog_returns": f"{DATA_PATH}\\catalog_returns.dat",
    "web_returns": f"{DATA_PATH}\\web_returns.dat",
    "web_sales": f"{DATA_PATH}\\web_sales.dat",
    "catalog_sales": f"{DATA_PATH}\\catalog_sales.dat",
    "store_sales": f"{DATA_PATH}\\store_sales.dat"
}

QUERY_PATH = f"{PROJECT_ROOT}\\test"
import tpc_schema as t
from config import DATA_FILES

def read(spark):

    # 读取所有TPC-DS表
    call_center = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.call_center_schema) \
        .csv(DATA_FILES["call_center"])

    customer_address = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_address_schema) \
        .csv(DATA_FILES["customer_address"])

    customer_demographics = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_demographics_schema) \
        .csv(DATA_FILES["customer_demographics"])

    date_dim = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.date_dim_schema) \
        .csv(DATA_FILES["date_dim"])

    warehouse = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.warehouse_schema) \
        .csv(DATA_FILES["warehouse"])

    ship_mode = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.ship_mode_schema) \
        .csv(DATA_FILES["ship_mode"])

    time_dim = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.time_dim_schema) \
        .csv(DATA_FILES["time_dim"])

    reason = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.reason_schema) \
        .csv(DATA_FILES["reason"])

    income_band = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.income_band_schema) \
        .csv(DATA_FILES["income_band"])

    item = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.item_schema) \
        .csv(DATA_FILES["item"])

    store = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_schema) \
        .csv(DATA_FILES["store"])

    customer = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_schema) \
        .csv(DATA_FILES["customer"])

    web_site = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_site_schema) \
        .csv(DATA_FILES["web_site"])

    store_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_returns_schema) \
        .csv(DATA_FILES["store_returns"])

    household_demographics = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.household_demographics_schema) \
        .csv(DATA_FILES["household_demographics"])

    web_page = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_page_schema) \
        .csv(DATA_FILES["web_page"])

    promotion = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.promotion_schema) \
        .csv(DATA_FILES["promotion"])

    catalog_page = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_page_schema) \
        .csv(DATA_FILES["catalog_page"])

    inventory = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.inventory_schema) \
        .csv(DATA_FILES["inventory"])

    catalog_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_returns_schema) \
        .csv(DATA_FILES["catalog_returns"])

    web_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_returns_schema) \
        .csv(DATA_FILES["web_returns"])

    web_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_sales_schema) \
        .csv(DATA_FILES["web_sales"])

    catalog_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_sales_schema) \
        .csv(DATA_FILES["catalog_sales"])

    store_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_sales_schema) \
        .csv(DATA_FILES["store_sales"])

    # 创建临时视图
    call_center.createOrReplaceTempView("call_center")
    customer_address.createOrReplaceTempView("customer_address")
    customer_demographics.createOrReplaceTempView("customer_demographics")
    date_dim.createOrReplaceTempView("date_dim")
    warehouse.createOrReplaceTempView("warehouse")
    ship_mode.createOrReplaceTempView("ship_mode")
    time_dim.createOrReplaceTempView("time_dim")
    reason.createOrReplaceTempView("reason")
    income_band.createOrReplaceTempView("income_band")
    item.createOrReplaceTempView("item")
    store.createOrReplaceTempView("store")
    customer.createOrReplaceTempView("customer")
    web_site.createOrReplaceTempView("web_site")
    store_returns.createOrReplaceTempView("store_returns")
    household_demographics.createOrReplaceTempView("household_demographics")
    web_page.createOrReplaceTempView("web_page")
    promotion.createOrReplaceTempView("promotion")
    catalog_page.createOrReplaceTempView("catalog_page")
    inventory.createOrReplaceTempView("inventory")
    catalog_returns.createOrReplaceTempView("catalog_returns")
    web_returns.createOrReplaceTempView("web_returns")
    web_sales.createOrReplaceTempView("web_sales")
    catalog_sales.createOrReplaceTempView("catalog_sales")
    store_sales.createOrReplaceTempView("store_sales")

    print("所有表加载完成！")

    return call_center,customer_address,customer_demographics,\
        date_dim,warehouse,ship_mode,time_dim,reason,income_band,\
        item,store,customer,web_site,store_returns,household_demographics,\
        web_page,promotion,catalog_page,inventory,catalog_returns,web_returns,\
        web_sales,catalog_sales,store_sales
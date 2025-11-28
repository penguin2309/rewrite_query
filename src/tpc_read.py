import tpc_schema as t

def read(spark):

    # 读取所有TPC-DS表
    call_center = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.call_center_schema) \
        .csv("C:\\tpcds-data\\call_center.dat")

    customer_address = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_address_schema) \
        .csv("C:\\tpcds-data\\customer_address.dat")

    customer_demographics = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_demographics_schema) \
        .csv("C:\\tpcds-data\\customer_demographics.dat")

    date_dim = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.date_dim_schema) \
        .csv("C:\\tpcds-data\\date_dim.dat")

    warehouse = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.warehouse_schema) \
        .csv("C:\\tpcds-data\\warehouse.dat")

    ship_mode = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.ship_mode_schema) \
        .csv("C:\\tpcds-data\\ship_mode.dat")

    time_dim = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.time_dim_schema) \
        .csv("C:\\tpcds-data\\time_dim.dat")

    reason = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.reason_schema) \
        .csv("C:\\tpcds-data\\reason.dat")

    income_band = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.income_band_schema) \
        .csv("C:\\tpcds-data\\income_band.dat")

    item = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.item_schema) \
        .csv("C:\\tpcds-data\\item.dat")

    store = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_schema) \
        .csv("C:\\tpcds-data\\store.dat")

    customer = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.customer_schema) \
        .csv("C:\\tpcds-data\\customer.dat")

    web_site = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_site_schema) \
        .csv("C:\\tpcds-data\\web_site.dat")

    store_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_returns_schema) \
        .csv("C:\\tpcds-data\\store_returns.dat")

    household_demographics = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.household_demographics_schema) \
        .csv("C:\\tpcds-data\\household_demographics.dat")

    web_page = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_page_schema) \
        .csv("C:\\tpcds-data\\web_page.dat")

    promotion = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.promotion_schema) \
        .csv("C:\\tpcds-data\\promotion.dat")

    catalog_page = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_page_schema) \
        .csv("C:\\tpcds-data\\catalog_page.dat")

    inventory = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.inventory_schema) \
        .csv("C:\\tpcds-data\\inventory.dat")

    catalog_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_returns_schema) \
        .csv("C:\\tpcds-data\\catalog_returns.dat")

    web_returns = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_returns_schema) \
        .csv("C:\\tpcds-data\\web_returns.dat")

    web_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.web_sales_schema) \
        .csv("C:\\tpcds-data\\web_sales.dat")

    catalog_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.catalog_sales_schema) \
        .csv("C:\\tpcds-data\\catalog_sales.dat")

    store_sales = spark.read \
        .option("header", "false") \
        .option("delimiter", "|") \
        .schema(t.store_sales_schema) \
        .csv("C:\\tpcds-data\\store_sales.dat")

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
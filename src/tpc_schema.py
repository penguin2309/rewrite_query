from pyspark.sql.types import *

# 所有表的Schema定义
dbgen_version_schema = StructType([
    StructField("dv_version", StringType(), True),
    StructField("dv_create_date", DateType(), True),
    StructField("dv_create_time", StringType(), True),  # Spark没有TimeType，用StringType
    StructField("dv_cmdline_args", StringType(), True)
])

customer_address_schema = StructType([
    StructField("ca_address_sk", IntegerType(), False),
    StructField("ca_address_id", StringType(), False),
    StructField("ca_street_number", StringType(), True),
    StructField("ca_street_name", StringType(), True),
    StructField("ca_street_type", StringType(), True),
    StructField("ca_suite_number", StringType(), True),
    StructField("ca_city", StringType(), True),
    StructField("ca_county", StringType(), True),
    StructField("ca_state", StringType(), True),
    StructField("ca_zip", StringType(), True),
    StructField("ca_country", StringType(), True),
    StructField("ca_gmt_offset", DecimalType(5, 2), True),
    StructField("ca_location_type", StringType(), True)
])

customer_demographics_schema = StructType([
    StructField("cd_demo_sk", IntegerType(), False),
    StructField("cd_gender", StringType(), True),
    StructField("cd_marital_status", StringType(), True),
    StructField("cd_education_status", StringType(), True),
    StructField("cd_purchase_estimate", IntegerType(), True),
    StructField("cd_credit_rating", StringType(), True),
    StructField("cd_dep_count", IntegerType(), True),
    StructField("cd_dep_employed_count", IntegerType(), True),
    StructField("cd_dep_college_count", IntegerType(), True)
])

date_dim_schema = StructType([
    StructField("d_date_sk", IntegerType(), False),
    StructField("d_date_id", StringType(), False),
    StructField("d_date", DateType(), True),
    StructField("d_month_seq", IntegerType(), True),
    StructField("d_week_seq", IntegerType(), True),
    StructField("d_quarter_seq", IntegerType(), True),
    StructField("d_year", IntegerType(), True),
    StructField("d_dow", IntegerType(), True),
    StructField("d_moy", IntegerType(), True),
    StructField("d_dom", IntegerType(), True),
    StructField("d_qoy", IntegerType(), True),
    StructField("d_fy_year", IntegerType(), True),
    StructField("d_fy_quarter_seq", IntegerType(), True),
    StructField("d_fy_week_seq", IntegerType(), True),
    StructField("d_day_name", StringType(), True),
    StructField("d_quarter_name", StringType(), True),
    StructField("d_holiday", StringType(), True),
    StructField("d_weekend", StringType(), True),
    StructField("d_following_holiday", StringType(), True),
    StructField("d_first_dom", IntegerType(), True),
    StructField("d_last_dom", IntegerType(), True),
    StructField("d_same_day_ly", IntegerType(), True),
    StructField("d_same_day_lq", IntegerType(), True),
    StructField("d_current_day", StringType(), True),
    StructField("d_current_week", StringType(), True),
    StructField("d_current_month", StringType(), True),
    StructField("d_current_quarter", StringType(), True),
    StructField("d_current_year", StringType(), True)
])

warehouse_schema = StructType([
    StructField("w_warehouse_sk", IntegerType(), False),
    StructField("w_warehouse_id", StringType(), False),
    StructField("w_warehouse_name", StringType(), True),
    StructField("w_warehouse_sq_ft", IntegerType(), True),
    StructField("w_street_number", StringType(), True),
    StructField("w_street_name", StringType(), True),
    StructField("w_street_type", StringType(), True),
    StructField("w_suite_number", StringType(), True),
    StructField("w_city", StringType(), True),
    StructField("w_county", StringType(), True),
    StructField("w_state", StringType(), True),
    StructField("w_zip", StringType(), True),
    StructField("w_country", StringType(), True),
    StructField("w_gmt_offset", DecimalType(5, 2), True)
])

ship_mode_schema = StructType([
    StructField("sm_ship_mode_sk", IntegerType(), False),
    StructField("sm_ship_mode_id", StringType(), False),
    StructField("sm_type", StringType(), True),
    StructField("sm_code", StringType(), True),
    StructField("sm_carrier", StringType(), True),
    StructField("sm_contract", StringType(), True)
])

time_dim_schema = StructType([
    StructField("t_time_sk", IntegerType(), False),
    StructField("t_time_id", StringType(), False),
    StructField("t_time", IntegerType(), True),
    StructField("t_hour", IntegerType(), True),
    StructField("t_minute", IntegerType(), True),
    StructField("t_second", IntegerType(), True),
    StructField("t_am_pm", StringType(), True),
    StructField("t_shift", StringType(), True),
    StructField("t_sub_shift", StringType(), True),
    StructField("t_meal_time", StringType(), True)
])

reason_schema = StructType([
    StructField("r_reason_sk", IntegerType(), False),
    StructField("r_reason_id", StringType(), False),
    StructField("r_reason_desc", StringType(), True)
])

income_band_schema = StructType([
    StructField("ib_income_band_sk", IntegerType(), False),
    StructField("ib_lower_bound", IntegerType(), True),
    StructField("ib_upper_bound", IntegerType(), True)
])

item_schema = StructType([
    StructField("i_item_sk", IntegerType(), False),
    StructField("i_item_id", StringType(), False),
    StructField("i_rec_start_date", DateType(), True),
    StructField("i_rec_end_date", DateType(), True),
    StructField("i_item_desc", StringType(), True),
    StructField("i_current_price", DecimalType(7, 2), True),
    StructField("i_wholesale_cost", DecimalType(7, 2), True),
    StructField("i_brand_id", IntegerType(), True),
    StructField("i_brand", StringType(), True),
    StructField("i_class_id", IntegerType(), True),
    StructField("i_class", StringType(), True),
    StructField("i_category_id", IntegerType(), True),
    StructField("i_category", StringType(), True),
    StructField("i_manufact_id", IntegerType(), True),
    StructField("i_manufact", StringType(), True),
    StructField("i_size", StringType(), True),
    StructField("i_formulation", StringType(), True),
    StructField("i_color", StringType(), True),
    StructField("i_units", StringType(), True),
    StructField("i_container", StringType(), True),
    StructField("i_manager_id", IntegerType(), True),
    StructField("i_product_name", StringType(), True)
])

store_schema = StructType([
    StructField("s_store_sk", IntegerType(), False),
    StructField("s_store_id", StringType(), False),
    StructField("s_rec_start_date", DateType(), True),
    StructField("s_rec_end_date", DateType(), True),
    StructField("s_closed_date_sk", IntegerType(), True),
    StructField("s_store_name", StringType(), True),
    StructField("s_number_employees", IntegerType(), True),
    StructField("s_floor_space", IntegerType(), True),
    StructField("s_hours", StringType(), True),
    StructField("s_manager", StringType(), True),
    StructField("s_market_id", IntegerType(), True),
    StructField("s_geography_class", StringType(), True),
    StructField("s_market_desc", StringType(), True),
    StructField("s_market_manager", StringType(), True),
    StructField("s_division_id", IntegerType(), True),
    StructField("s_division_name", StringType(), True),
    StructField("s_company_id", IntegerType(), True),
    StructField("s_company_name", StringType(), True),
    StructField("s_street_number", StringType(), True),
    StructField("s_street_name", StringType(), True),
    StructField("s_street_type", StringType(), True),
    StructField("s_suite_number", StringType(), True),
    StructField("s_city", StringType(), True),
    StructField("s_county", StringType(), True),
    StructField("s_state", StringType(), True),
    StructField("s_zip", StringType(), True),
    StructField("s_country", StringType(), True),
    StructField("s_gmt_offset", DecimalType(5, 2), True),
    StructField("s_tax_precentage", DecimalType(5, 2), True)
])

call_center_schema = StructType([
    StructField("cc_call_center_sk", IntegerType(), False),
    StructField("cc_call_center_id", StringType(), False),
    StructField("cc_rec_start_date", DateType(), True),
    StructField("cc_rec_end_date", DateType(), True),
    StructField("cc_closed_date_sk", IntegerType(), True),
    StructField("cc_open_date_sk", IntegerType(), True),
    StructField("cc_name", StringType(), True),
    StructField("cc_class", StringType(), True),
    StructField("cc_employees", IntegerType(), True),
    StructField("cc_sq_ft", IntegerType(), True),
    StructField("cc_hours", StringType(), True),
    StructField("cc_manager", StringType(), True),
    StructField("cc_mkt_id", IntegerType(), True),
    StructField("cc_mkt_class", StringType(), True),
    StructField("cc_mkt_desc", StringType(), True),
    StructField("cc_market_manager", StringType(), True),
    StructField("cc_division", IntegerType(), True),
    StructField("cc_division_name", StringType(), True),
    StructField("cc_company", IntegerType(), True),
    StructField("cc_company_name", StringType(), True),
    StructField("cc_street_number", StringType(), True),
    StructField("cc_street_name", StringType(), True),
    StructField("cc_street_type", StringType(), True),
    StructField("cc_suite_number", StringType(), True),
    StructField("cc_city", StringType(), True),
    StructField("cc_county", StringType(), True),
    StructField("cc_state", StringType(), True),
    StructField("cc_zip", StringType(), True),
    StructField("cc_country", StringType(), True),
    StructField("cc_gmt_offset", DecimalType(5, 2), True),
    StructField("cc_tax_percentage", DecimalType(5, 2), True)
])

customer_schema = StructType([
    StructField("c_customer_sk", IntegerType(), False),
    StructField("c_customer_id", StringType(), False),
    StructField("c_current_cdemo_sk", IntegerType(), True),
    StructField("c_current_hdemo_sk", IntegerType(), True),
    StructField("c_current_addr_sk", IntegerType(), True),
    StructField("c_first_shipto_date_sk", IntegerType(), True),
    StructField("c_first_sales_date_sk", IntegerType(), True),
    StructField("c_salutation", StringType(), True),
    StructField("c_first_name", StringType(), True),
    StructField("c_last_name", StringType(), True),
    StructField("c_preferred_cust_flag", StringType(), True),
    StructField("c_birth_day", IntegerType(), True),
    StructField("c_birth_month", IntegerType(), True),
    StructField("c_birth_year", IntegerType(), True),
    StructField("c_birth_country", StringType(), True),
    StructField("c_login", StringType(), True),
    StructField("c_email_address", StringType(), True),
    StructField("c_last_review_date_sk", IntegerType(), True)
])

web_site_schema = StructType([
    StructField("web_site_sk", IntegerType(), False),
    StructField("web_site_id", StringType(), False),
    StructField("web_rec_start_date", DateType(), True),
    StructField("web_rec_end_date", DateType(), True),
    StructField("web_name", StringType(), True),
    StructField("web_open_date_sk", IntegerType(), True),
    StructField("web_close_date_sk", IntegerType(), True),
    StructField("web_class", StringType(), True),
    StructField("web_manager", StringType(), True),
    StructField("web_mkt_id", IntegerType(), True),
    StructField("web_mkt_class", StringType(), True),
    StructField("web_mkt_desc", StringType(), True),
    StructField("web_market_manager", StringType(), True),
    StructField("web_company_id", IntegerType(), True),
    StructField("web_company_name", StringType(), True),
    StructField("web_street_number", StringType(), True),
    StructField("web_street_name", StringType(), True),
    StructField("web_street_type", StringType(), True),
    StructField("web_suite_number", StringType(), True),
    StructField("web_city", StringType(), True),
    StructField("web_county", StringType(), True),
    StructField("web_state", StringType(), True),
    StructField("web_zip", StringType(), True),
    StructField("web_country", StringType(), True),
    StructField("web_gmt_offset", DecimalType(5, 2), True),
    StructField("web_tax_percentage", DecimalType(5, 2), True)
])

store_returns_schema = StructType([
    StructField("sr_returned_date_sk", IntegerType(), True),
    StructField("sr_return_time_sk", IntegerType(), True),
    StructField("sr_item_sk", IntegerType(), False),
    StructField("sr_customer_sk", IntegerType(), True),
    StructField("sr_cdemo_sk", IntegerType(), True),
    StructField("sr_hdemo_sk", IntegerType(), True),
    StructField("sr_addr_sk", IntegerType(), True),
    StructField("sr_store_sk", IntegerType(), True),
    StructField("sr_reason_sk", IntegerType(), True),
    StructField("sr_ticket_number", IntegerType(), False),
    StructField("sr_return_quantity", IntegerType(), True),
    StructField("sr_return_amt", DecimalType(7, 2), True),
    StructField("sr_return_tax", DecimalType(7, 2), True),
    StructField("sr_return_amt_inc_tax", DecimalType(7, 2), True),
    StructField("sr_fee", DecimalType(7, 2), True),
    StructField("sr_return_ship_cost", DecimalType(7, 2), True),
    StructField("sr_refunded_cash", DecimalType(7, 2), True),
    StructField("sr_reversed_charge", DecimalType(7, 2), True),
    StructField("sr_store_credit", DecimalType(7, 2), True),
    StructField("sr_net_loss", DecimalType(7, 2), True)
])

household_demographics_schema = StructType([
    StructField("hd_demo_sk", IntegerType(), False),
    StructField("hd_income_band_sk", IntegerType(), True),
    StructField("hd_buy_potential", StringType(), True),
    StructField("hd_dep_count", IntegerType(), True),
    StructField("hd_vehicle_count", IntegerType(), True)
])

web_page_schema = StructType([
    StructField("wp_web_page_sk", IntegerType(), False),
    StructField("wp_web_page_id", StringType(), False),
    StructField("wp_rec_start_date", DateType(), True),
    StructField("wp_rec_end_date", DateType(), True),
    StructField("wp_creation_date_sk", IntegerType(), True),
    StructField("wp_access_date_sk", IntegerType(), True),
    StructField("wp_autogen_flag", StringType(), True),
    StructField("wp_customer_sk", IntegerType(), True),
    StructField("wp_url", StringType(), True),
    StructField("wp_type", StringType(), True),
    StructField("wp_char_count", IntegerType(), True),
    StructField("wp_link_count", IntegerType(), True),
    StructField("wp_image_count", IntegerType(), True),
    StructField("wp_max_ad_count", IntegerType(), True)
])

promotion_schema = StructType([
    StructField("p_promo_sk", IntegerType(), False),
    StructField("p_promo_id", StringType(), False),
    StructField("p_start_date_sk", IntegerType(), True),
    StructField("p_end_date_sk", IntegerType(), True),
    StructField("p_item_sk", IntegerType(), True),
    StructField("p_cost", DecimalType(15, 2), True),
    StructField("p_response_target", IntegerType(), True),
    StructField("p_promo_name", StringType(), True),
    StructField("p_channel_dmail", StringType(), True),
    StructField("p_channel_email", StringType(), True),
    StructField("p_channel_catalog", StringType(), True),
    StructField("p_channel_tv", StringType(), True),
    StructField("p_channel_radio", StringType(), True),
    StructField("p_channel_press", StringType(), True),
    StructField("p_channel_event", StringType(), True),
    StructField("p_channel_demo", StringType(), True),
    StructField("p_channel_details", StringType(), True),
    StructField("p_purpose", StringType(), True),
    StructField("p_discount_active", StringType(), True)
])

catalog_page_schema = StructType([
    StructField("cp_catalog_page_sk", IntegerType(), False),
    StructField("cp_catalog_page_id", StringType(), False),
    StructField("cp_start_date_sk", IntegerType(), True),
    StructField("cp_end_date_sk", IntegerType(), True),
    StructField("cp_department", StringType(), True),
    StructField("cp_catalog_number", IntegerType(), True),
    StructField("cp_catalog_page_number", IntegerType(), True),
    StructField("cp_description", StringType(), True),
    StructField("cp_type", StringType(), True)
])

inventory_schema = StructType([
    StructField("inv_date_sk", IntegerType(), False),
    StructField("inv_item_sk", IntegerType(), False),
    StructField("inv_warehouse_sk", IntegerType(), False),
    StructField("inv_quantity_on_hand", IntegerType(), True)
])

catalog_returns_schema = StructType([
    StructField("cr_returned_date_sk", IntegerType(), True),
    StructField("cr_returned_time_sk", IntegerType(), True),
    StructField("cr_item_sk", IntegerType(), False),
    StructField("cr_refunded_customer_sk", IntegerType(), True),
    StructField("cr_refunded_cdemo_sk", IntegerType(), True),
    StructField("cr_refunded_hdemo_sk", IntegerType(), True),
    StructField("cr_refunded_addr_sk", IntegerType(), True),
    StructField("cr_returning_customer_sk", IntegerType(), True),
    StructField("cr_returning_cdemo_sk", IntegerType(), True),
    StructField("cr_returning_hdemo_sk", IntegerType(), True),
    StructField("cr_returning_addr_sk", IntegerType(), True),
    StructField("cr_call_center_sk", IntegerType(), True),
    StructField("cr_catalog_page_sk", IntegerType(), True),
    StructField("cr_ship_mode_sk", IntegerType(), True),
    StructField("cr_warehouse_sk", IntegerType(), True),
    StructField("cr_reason_sk", IntegerType(), True),
    StructField("cr_order_number", IntegerType(), False),
    StructField("cr_return_quantity", IntegerType(), True),
    StructField("cr_return_amount", DecimalType(7, 2), True),
    StructField("cr_return_tax", DecimalType(7, 2), True),
    StructField("cr_return_amt_inc_tax", DecimalType(7, 2), True),
    StructField("cr_fee", DecimalType(7, 2), True),
    StructField("cr_return_ship_cost", DecimalType(7, 2), True),
    StructField("cr_refunded_cash", DecimalType(7, 2), True),
    StructField("cr_reversed_charge", DecimalType(7, 2), True),
    StructField("cr_store_credit", DecimalType(7, 2), True),
    StructField("cr_net_loss", DecimalType(7, 2), True)
])

web_returns_schema = StructType([
    StructField("wr_returned_date_sk", IntegerType(), True),
    StructField("wr_returned_time_sk", IntegerType(), True),
    StructField("wr_item_sk", IntegerType(), False),
    StructField("wr_refunded_customer_sk", IntegerType(), True),
    StructField("wr_refunded_cdemo_sk", IntegerType(), True),
    StructField("wr_refunded_hdemo_sk", IntegerType(), True),
    StructField("wr_refunded_addr_sk", IntegerType(), True),
    StructField("wr_returning_customer_sk", IntegerType(), True),
    StructField("wr_returning_cdemo_sk", IntegerType(), True),
    StructField("wr_returning_hdemo_sk", IntegerType(), True),
    StructField("wr_returning_addr_sk", IntegerType(), True),
    StructField("wr_web_page_sk", IntegerType(), True),
    StructField("wr_reason_sk", IntegerType(), True),
    StructField("wr_order_number", IntegerType(), False),
    StructField("wr_return_quantity", IntegerType(), True),
    StructField("wr_return_amt", DecimalType(7, 2), True),
    StructField("wr_return_tax", DecimalType(7, 2), True),
    StructField("wr_return_amt_inc_tax", DecimalType(7, 2), True),
    StructField("wr_fee", DecimalType(7, 2), True),
    StructField("wr_return_ship_cost", DecimalType(7, 2), True),
    StructField("wr_refunded_cash", DecimalType(7, 2), True),
    StructField("wr_reversed_charge", DecimalType(7, 2), True),
    StructField("wr_account_credit", DecimalType(7, 2), True),
    StructField("wr_net_loss", DecimalType(7, 2), True)
])

web_sales_schema = StructType([
    StructField("ws_sold_date_sk", IntegerType(), True),
    StructField("ws_sold_time_sk", IntegerType(), True),
    StructField("ws_ship_date_sk", IntegerType(), True),
    StructField("ws_item_sk", IntegerType(), False),
    StructField("ws_bill_customer_sk", IntegerType(), True),
    StructField("ws_bill_cdemo_sk", IntegerType(), True),
    StructField("ws_bill_hdemo_sk", IntegerType(), True),
    StructField("ws_bill_addr_sk", IntegerType(), True),
    StructField("ws_ship_customer_sk", IntegerType(), True),
    StructField("ws_ship_cdemo_sk", IntegerType(), True),
    StructField("ws_ship_hdemo_sk", IntegerType(), True),
    StructField("ws_ship_addr_sk", IntegerType(), True),
    StructField("ws_web_page_sk", IntegerType(), True),
    StructField("ws_web_site_sk", IntegerType(), True),
    StructField("ws_ship_mode_sk", IntegerType(), True),
    StructField("ws_warehouse_sk", IntegerType(), True),
    StructField("ws_promo_sk", IntegerType(), True),
    StructField("ws_order_number", IntegerType(), False),
    StructField("ws_quantity", IntegerType(), True),
    StructField("ws_wholesale_cost", DecimalType(7, 2), True),
    StructField("ws_list_price", DecimalType(7, 2), True),
    StructField("ws_sales_price", DecimalType(7, 2), True),
    StructField("ws_ext_discount_amt", DecimalType(7, 2), True),
    StructField("ws_ext_sales_price", DecimalType(7, 2), True),
    StructField("ws_ext_wholesale_cost", DecimalType(7, 2), True),
    StructField("ws_ext_list_price", DecimalType(7, 2), True),
    StructField("ws_ext_tax", DecimalType(7, 2), True),
    StructField("ws_coupon_amt", DecimalType(7, 2), True),
    StructField("ws_ext_ship_cost", DecimalType(7, 2), True),
    StructField("ws_net_paid", DecimalType(7, 2), True),
    StructField("ws_net_paid_inc_tax", DecimalType(7, 2), True),
    StructField("ws_net_paid_inc_ship", DecimalType(7, 2), True),
    StructField("ws_net_paid_inc_ship_tax", DecimalType(7, 2), True),
    StructField("ws_net_profit", DecimalType(7, 2), True)
])

catalog_sales_schema = StructType([
    StructField("cs_sold_date_sk", IntegerType(), True),
    StructField("cs_sold_time_sk", IntegerType(), True),
    StructField("cs_ship_date_sk", IntegerType(), True),
    StructField("cs_bill_customer_sk", IntegerType(), True),
    StructField("cs_bill_cdemo_sk", IntegerType(), True),
    StructField("cs_bill_hdemo_sk", IntegerType(), True),
    StructField("cs_bill_addr_sk", IntegerType(), True),
    StructField("cs_ship_customer_sk", IntegerType(), True),
    StructField("cs_ship_cdemo_sk", IntegerType(), True),
    StructField("cs_ship_hdemo_sk", IntegerType(), True),
    StructField("cs_ship_addr_sk", IntegerType(), True),
    StructField("cs_call_center_sk", IntegerType(), True),
    StructField("cs_catalog_page_sk", IntegerType(), True),
    StructField("cs_ship_mode_sk", IntegerType(), True),
    StructField("cs_warehouse_sk", IntegerType(), True),
    StructField("cs_item_sk", IntegerType(), False),
    StructField("cs_promo_sk", IntegerType(), True),
    StructField("cs_order_number", IntegerType(), False),
    StructField("cs_quantity", IntegerType(), True),
    StructField("cs_wholesale_cost", DecimalType(7, 2), True),
    StructField("cs_list_price", DecimalType(7, 2), True),
    StructField("cs_sales_price", DecimalType(7, 2), True),
    StructField("cs_ext_discount_amt", DecimalType(7, 2), True),
    StructField("cs_ext_sales_price", DecimalType(7, 2), True),
    StructField("cs_ext_wholesale_cost", DecimalType(7, 2), True),
    StructField("cs_ext_list_price", DecimalType(7, 2), True),
    StructField("cs_ext_tax", DecimalType(7, 2), True),
    StructField("cs_coupon_amt", DecimalType(7, 2), True),
    StructField("cs_ext_ship_cost", DecimalType(7, 2), True),
    StructField("cs_net_paid", DecimalType(7, 2), True),
    StructField("cs_net_paid_inc_tax", DecimalType(7, 2), True),
    StructField("cs_net_paid_inc_ship", DecimalType(7, 2), True),
    StructField("cs_net_paid_inc_ship_tax", DecimalType(7, 2), True),
    StructField("cs_net_profit", DecimalType(7, 2), True)
])

store_sales_schema = StructType([
    StructField("ss_sold_date_sk", IntegerType(), True),
    StructField("ss_sold_time_sk", IntegerType(), True),
    StructField("ss_item_sk", IntegerType(), False),
    StructField("ss_customer_sk", IntegerType(), True),
    StructField("ss_cdemo_sk", IntegerType(), True),
    StructField("ss_hdemo_sk", IntegerType(), True),
    StructField("ss_addr_sk", IntegerType(), True),
    StructField("ss_store_sk", IntegerType(), True),
    StructField("ss_promo_sk", IntegerType(), True),
    StructField("ss_ticket_number", IntegerType(), False),
    StructField("ss_quantity", IntegerType(), True),
    StructField("ss_wholesale_cost", DecimalType(7, 2), True),
    StructField("ss_list_price", DecimalType(7, 2), True),
    StructField("ss_sales_price", DecimalType(7, 2), True),
    StructField("ss_ext_discount_amt", DecimalType(7, 2), True),
    StructField("ss_ext_sales_price", DecimalType(7, 2), True),
    StructField("ss_ext_wholesale_cost", DecimalType(7, 2), True),
    StructField("ss_ext_list_price", DecimalType(7, 2), True),
    StructField("ss_ext_tax", DecimalType(7, 2), True),
    StructField("ss_coupon_amt", DecimalType(7, 2), True),
    StructField("ss_net_paid", DecimalType(7, 2), True),
    StructField("ss_net_paid_inc_tax", DecimalType(7, 2), True),
    StructField("ss_net_profit", DecimalType(7, 2), True)
])

# 创建表名和schema的映射字典
table_schemas = {
    "dbgen_version": dbgen_version_schema,
    "customer_address": customer_address_schema,
    "customer_demographics": customer_demographics_schema,
    "date_dim": date_dim_schema,
    "warehouse": warehouse_schema,
    "ship_mode": ship_mode_schema,
    "time_dim": time_dim_schema,
    "reason": reason_schema,
    "income_band": income_band_schema,
    "item": item_schema,
    "store": store_schema,
    "call_center": call_center_schema,
    "customer": customer_schema,
    "web_site": web_site_schema,
    "store_returns": store_returns_schema,
    "household_demographics": household_demographics_schema,
    "web_page": web_page_schema,
    "promotion": promotion_schema,
    "catalog_page": catalog_page_schema,
    "inventory": inventory_schema,
    "catalog_returns": catalog_returns_schema,
    "web_returns": web_returns_schema,
    "web_sales": web_sales_schema,
    "catalog_sales": catalog_sales_schema,
    "store_sales": store_sales_schema
}

sqls= ["""
create table dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            time                          ,
    dv_cmdline_args           varchar(200)                  
);
""", """
create table customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      ,
    primary key (ca_address_sk)
);
""", """
create table customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);
""", """
create table date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);
""", """
create table warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
);
""", """
create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
);
""", """
create table time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
);
""", """
create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
);
""", """
create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);
""", """
create table item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);
""", """
create table store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
);
""", """
create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)
);
""", """
create table customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date_sk     integer                       ,
    primary key (c_customer_sk)
);
""", """
create table web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
);
""", """
create table store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)
);
""", """
create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
);
""", """
create table web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
);
""", """
create table promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)
);
""", """
create table catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)
);
""", """
create table inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);
""", """
create table catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)
);
""", """
create table web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)
);
""", """
create table web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)
);
""", """
create table catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
);
""", """
create table store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);
"""]

tables_name = [
    "dbgen_version", "customer_address", "customer_demographics", "date_dim",
    "warehouse", "ship_mode", "time_dim", "reason", "income_band", "item",
    "store", "call_center", "customer", "web_site", "store_returns",
    "household_demographics", "web_page", "promotion", "catalog_page",
    "inventory", "catalog_returns", "web_returns", "web_sales",
    "catalog_sales", "store_sales"
]

primary_keys = [
    (),  # dbgen_version - 无主键
    ("ca_address_sk",),  # customer_address
    ("cd_demo_sk",),  # customer_demographics
    ("d_date_sk",),  # date_dim
    ("w_warehouse_sk",),  # warehouse
    ("sm_ship_mode_sk",),  # ship_mode
    ("t_time_sk",),  # time_dim
    ("r_reason_sk",),  # reason
    ("ib_income_band_sk",),  # income_band
    ("i_item_sk",),  # item
    ("s_store_sk",),  # store
    ("cc_call_center_sk",),  # call_center
    ("c_customer_sk",),  # customer
    ("web_site_sk",),  # web_site
    ("sr_item_sk", "sr_ticket_number"),  # store_returns
    ("hd_demo_sk",),  # household_demographics
    ("wp_web_page_sk",),  # web_page
    ("p_promo_sk",),  # promotion
    ("cp_catalog_page_sk",),  # catalog_page
    ("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"),  # inventory
    ("cr_item_sk", "cr_order_number"),  # catalog_returns
    ("wr_item_sk", "wr_order_number"),  # web_returns
    ("ws_item_sk", "ws_order_number"),  # web_sales
    ("cs_item_sk", "cs_order_number"),  # catalog_sales
    ("ss_item_sk", "ss_ticket_number")  # store_sales
]
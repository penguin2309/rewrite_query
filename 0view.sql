CREATE TABLE mv_001 USING CSV
LOCATION '/tmp/ecse/mv_001/'
AS SELECT COUNT(*) cnt
FROM store_sales
JOIN household_demographics ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
JOIN time_dim ON time_dim.t_time_sk = store_sales.ss_time_sk;

CREATE TABLE mv_002 USING CSV
LOCATION '/tmp/ecse/mv_002/'
AS SELECT d_year, web_site_id, ws_bill_customer_sk, ws_item_sk, COUNT(*) cnt, SUM(COALESCE(wr_return_amt, 0)) returns, SUM(ws_ext_sales_price) sales, SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) profit, SUM(ws_quantity) ws_qty, SUM(ws_sales_price) ws_sp, SUM(ws_wholesale_cost) ws_wc
FROM web_sales
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
JOIN web_returns ON web_returns.wr_web_returns_sk = web_sales.ws_wr_sk
GROUP BY d_year, web_site_id, ws_bill_customer_sk, ws_item_sk;

CREATE TABLE mv_003 USING CSV
LOCATION '/tmp/ecse/mv_003/'
AS SELECT cp_catalog_page_id, cs_bill_customer_sk, cs_item_sk, d_year, i_item_id, w_state, COUNT(*) cnt, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) < TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_before, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) >= TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_after, SUM(COALESCE(cr_return_amount, 0)) returns, SUM(cs_ext_sales_price) sales, SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) profit, SUM(cs_quantity) cs_qty, SUM(cs_sales_price) cs_sp, SUM(cs_wholesale_cost) cs_wc
FROM catalog_sales
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY cp_catalog_page_id, cs_bill_customer_sk, cs_item_sk, d_year, i_item_id, w_state;

CREATE TABLE mv_004 USING CSV
LOCATION '/tmp/ecse/mv_004/'
AS SELECT d_year, s_store_id, ss_customer_sk, ss_item_sk, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_quantity) ss_qty, SUM(ss_sales_price) ss_sp, SUM(ss_wholesale_cost) ss_wc
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY d_year, s_store_id, ss_customer_sk, ss_item_sk;

CREATE TABLE mv_005 USING CSV
LOCATION '/tmp/ecse/mv_005/'
AS SELECT i_item_id, d_year,s_store_sk, sr_customer_sk, sr_store_sk, SUM(SR_RETURN_AMT_INC_TAX) ctr_total_return, SUM(sr_net_loss) profit_loss, SUM(sr_return_amt) returns, SUM(sr_return_quantity) sr_item_qty,SUM(sr_fee) tot_returns,COUNT_BIG(*) cnt_big
FROM store_returns
JOIN date_dim ON date_dim.d_date_sk = store_returns.sr_returned_date_sk
GROUP BY i_item_id, s_store_sk, sr_customer_sk, sr_store_sk,d_year;

CREATE TABLE mv_006 USING CSV
LOCATION '/tmp/ecse/mv_006/'
AS SELECT d_moy, i_item_id, i_item_sk, w_warehouse_name, w_warehouse_sk, COUNT(*) cnt
FROM inventory
JOIN date_dim ON date_dim.d_date_sk = inventory.inv_date_sk
JOIN item ON item.i_item_sk = inventory.inv_i_sk
JOIN warehouse ON warehouse.w_warehouse_sk = inventory.inv_warehouse_sk
GROUP BY d_moy, i_item_id, i_item_sk, w_warehouse_name, w_warehouse_sk;

CREATE TABLE mv_007 USING CSV
LOCATION '/tmp/ecse/mv_007/'
AS SELECT i_brand, i_brand_id, i_manufact, i_manufact_id, s_store_id, s_store_name, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) fri_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) mon_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) sat_sales, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) sun_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) thu_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) wed_sales, SUM(ss_ext_sales_price) ext_price
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY i_brand, i_brand_id, i_manufact, i_manufact_id, s_store_id, s_store_name;



CREATE TABLE mv_012 USING CSV
LOCATION '/tmp/ecse/mv_012/'
AS SELECT i_item_id, i_manufact_id, COUNT(*) cnt, SUM(ss_ext_sales_price) total_sales
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY i_item_id, i_manufact_id;

CREATE TABLE mv_013 USING CSV
LOCATION '/tmp/ecse/mv_013/'
AS SELECT i_item_id, i_manufact_id, AVG(TRY_CAST(c_birth_year AS DECIMAL(12, 2))) agg6, AVG(TRY_CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) agg7, AVG(TRY_CAST(cs_coupon_amt AS DECIMAL(12, 2))) agg3, AVG(TRY_CAST(cs_list_price AS DECIMAL(12, 2))) agg2, AVG(TRY_CAST(cs_net_profit AS DECIMAL(12, 2))) agg5, AVG(TRY_CAST(cs_quantity AS DECIMAL(12, 2))) agg1, AVG(TRY_CAST(cs_sales_price AS DECIMAL(12, 2))) agg4, SUM(cs_ext_sales_price) total_sales
FROM catalog_sales
JOIN customer_address ON customer_address.ca_address_sk = catalog_sales.cs_addr_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
GROUP BY i_item_id, i_manufact_id;

CREATE TABLE mv_014 USING CSV
LOCATION '/tmp/ecse/mv_014/'
AS SELECT ca_city, ca_zip, i_item_id, i_manufact_id, SUM(ws_ext_sales_price) total_sales, SUM(ws_sales_price) sum_val
FROM web_sales
JOIN customer_address ON customer_address.ca_address_sk = web_sales.ws_addr_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
JOIN item ON item.i_item_sk = web_sales.ws_i_sk
GROUP BY ca_city, ca_zip, i_item_id, i_manufact_id;

CREATE TABLE mv_016 USING CSV
LOCATION '/tmp/ecse/mv_016/'
AS SELECT d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_manager_id, i_manufact_id, s_company_name, s_store_name, AVG(SUM(ss_sales_price)) avg_monthly_sales, COUNT(*) cnt, SUM(ss_sales_price) avg_monthly_sales, SUM(ss_sales_price) sum_sales
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_manager_id, i_manufact_id, s_company_name, s_store_name;

CREATE TABLE mv_017 USING CSV
LOCATION '/tmp/ecse/mv_017/'
AS SELECT cp_catalog_page_id, i_item_id, w_state, COUNT(*) cnt, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) < TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_before, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) >= TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_after, SUM(COALESCE(cr_return_amount, 0)) returns, SUM(cs_ext_sales_price) sales, SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) profit
FROM catalog_sales
JOIN catalog_returns ON catalog_returns.cr_catalog_returns_sk = catalog_sales.cs_cr_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
GROUP BY cp_catalog_page_id, i_item_id, w_state;

CREATE TABLE mv_018 USING CSV
LOCATION '/tmp/ecse/mv_018/'
AS SELECT s_store_id, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store_returns ON store_returns.sr_store_returns_sk = store_sales.ss_sr_sk
GROUP BY s_store_id;

CREATE TABLE mv_019 USING CSV
LOCATION '/tmp/ecse/mv_019/'
AS SELECT web_site_id, COUNT(*) cnt, SUM(COALESCE(wr_return_amt, 0)) returns, SUM(ws_ext_sales_price) sales, SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) profit
FROM web_sales
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
JOIN item ON item.i_item_sk = web_sales.ws_i_sk
JOIN web_returns ON web_returns.wr_web_returns_sk = web_sales.ws_wr_sk
GROUP BY web_site_id;


CREATE TABLE mv_021 USING CSV
LOCATION '/tmp/ecse/mv_021/'
AS SELECT s_store_id, AVG(ss_coupon_amt) agg3, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_sales_price) agg4, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY s_store_id;


CREATE TABLE mv_031 USING CSV
LOCATION '/tmp/ecse/mv_031/'
AS SELECT ca_city, ca_county, d_qoy, d_year, i_item_id, i_manufact_id, ss_addr_sk, ss_customer_sk, ss_ticket_number, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_sales_price) store_sales, SUM(ss_ext_sales_price) total_sales, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY ca_city, ca_county, d_qoy, d_year, i_item_id, i_manufact_id, ss_addr_sk, ss_customer_sk, ss_ticket_number;

CREATE TABLE mv_032 USING CSV
LOCATION '/tmp/ecse/mv_032/'
AS SELECT ca_city, ca_county, ca_zip, d_qoy, d_year, i_item_id, i_manufact_id, r_reason_desc, AVG(wr_fee) avg_val, AVG(wr_refunded_cash) avg_val, AVG(ws_quantity) avg_val, SUM(ws_ext_sales_price) total_sales, SUM(ws_ext_sales_price) web_sales, SUM(ws_sales_price) sum_val
FROM web_sales
JOIN customer_address ON customer_address.ca_address_sk = web_sales.ws_addr_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
GROUP BY ca_city, ca_county, ca_zip, d_qoy, d_year, i_item_id, i_manufact_id, r_reason_desc;

CREATE TABLE mv_033 USING CSV
LOCATION '/tmp/ecse/mv_033/'
AS SELECT c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year, COUNT(*) cnt, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / NULLIF(2, 0)) year_total, SUM(ss_ext_list_price - ss_ext_discount_amt) year_total
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year;

CREATE TABLE mv_034 USING CSV
LOCATION '/tmp/ecse/mv_034/'
AS SELECT c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year, COUNT(*) cnt, SUM((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / NULLIF(2, 0))) year_total, SUM(ws_ext_list_price - ws_ext_discount_amt) year_total
FROM web_sales
JOIN customer ON customer.c_customer_sk = web_sales.ws_c_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
GROUP BY c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year;

CREATE TABLE mv_035 USING CSV
LOCATION '/tmp/ecse/mv_035/'
AS SELECT ca_city, ca_county, d_qoy, d_year, i_item_id, i_manufact_id, ws_bill_addr_sk, AVG(ws_ext_sales_price) avg_val, AVG(ws_ext_wholesale_cost) avg_val, AVG(ws_quantity) avg_val, COUNT(*) cnt, SUM(ws_ext_sales_price) web_sales, SUM(ws_ext_sales_price) total_sales, SUM(ws_ext_wholesale_cost) sum_val, SUM(ws_quantity) sum_val
FROM web_sales
JOIN customer_address ON customer_address.ca_address_sk = ws_bill_addr_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
GROUP BY ca_city, ca_county, d_qoy, d_year, i_item_id, i_manufact_id,ws_bill_addr_sk;

CREATE TABLE mv_036 USING CSV
LOCATION '/tmp/ecse/mv_036/'
AS SELECT COUNT(*) cnt
FROM web_sales
JOIN catalog_sales ON catalog_sales.cs_catalog_sales_sk = web_sales.ws_cs_sk
JOIN customer ON customer.c_customer_sk = web_sales.ws_c_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
JOIN store_sales ON store_sales.ss_store_sales_sk = web_sales.ws_ss_sk;

CREATE TABLE mv_037 USING CSV
LOCATION '/tmp/ecse/mv_037/'
AS SELECT d_moy, i_item_id, i_item_sk, w_warehouse_name, w_warehouse_sk, AVG(inv_quantity_on_hand) qoh, COUNT(*) cnt
FROM inventory
JOIN date_dim ON date_dim.d_date_sk = inventory.inv_date_sk
JOIN item ON item.i_item_sk = inventory.inv_i_sk
GROUP BY d_moy, i_item_id, i_item_sk, w_warehouse_name, w_warehouse_sk;

CREATE TABLE mv_038 USING CSV
LOCATION '/tmp/ecse/mv_038/'
AS SELECT ca_zip, AVG(TRY_CAST(c_birth_year AS DECIMAL(12, 2))) agg6, AVG(TRY_CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) agg7, AVG(TRY_CAST(cs_coupon_amt AS DECIMAL(12, 2))) agg3, AVG(TRY_CAST(cs_list_price AS DECIMAL(12, 2))) agg2, AVG(TRY_CAST(cs_net_profit AS DECIMAL(12, 2))) agg5, AVG(TRY_CAST(cs_quantity AS DECIMAL(12, 2))) agg1, AVG(TRY_CAST(cs_sales_price AS DECIMAL(12, 2))) agg4, SUM(cs_sales_price) sum_val
FROM catalog_sales
JOIN customer ON customer.c_customer_sk = catalog_sales.cs_c_sk
JOIN customer_address ON customer_address.ca_address_sk = catalog_sales.cs_addr_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY ca_zip;

CREATE TABLE mv_040 USING CSV
LOCATION '/tmp/ecse/mv_040/'
AS SELECT c_customer_sk, COUNT(*) cnt, MAX(csales) tpcds_cmax
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY c_customer_sk;

CREATE TABLE mv_041 USING CSV
LOCATION '/tmp/ecse/mv_041/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_customer_sk, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_name, s_zip, COUNT(*) cnt, MAX(csales) tpcds_cmax, SUM(ss_coupon_amt) s3, SUM(ss_list_price) s2, SUM(ss_net_profit) netpaid, SUM(ss_quantity * ss_sales_price) ssales, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_customer_sk, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_name, s_zip;

CREATE TABLE mv_042 USING CSV
LOCATION '/tmp/ecse/mv_042/'
AS SELECT AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk;

CREATE TABLE mv_043 USING CSV
LOCATION '/tmp/ecse/mv_043/'
AS SELECT ca_city, s_store_id, s_store_name, s_store_sk, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city, AVG(ss_coupon_amt) agg3, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_quantity) avg_val, AVG(ss_sales_price) agg4, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_net_profit) profit, SUM(ss_net_profit) rank_within_parent, SUM(ss_net_profit) sum_val, SUM(ss_net_profit) total_sum, SUM(ss_quantity) sum_val
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ca_city, s_store_id, s_store_name, s_store_sk, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city;

CREATE TABLE mv_044 USING CSV
LOCATION '/tmp/ecse/mv_044/'
AS SELECT ca_state, cr_call_center_sk, cr_returning_customer_sk, i_item_id, SUM(cr_net_loss) profit_loss, SUM(cr_return_amount) returns, SUM(cr_return_amt_inc_tax) ctr_total_return, SUM(cr_return_quantity) cr_item_qty
FROM catalog_returns
JOIN date_dim ON date_dim.d_date_sk = catalog_returns.cr_sold_date_sk
GROUP BY ca_state, cr_call_center_sk, cr_returning_customer_sk, i_item_id;

CREATE TABLE mv_046 USING CSV
LOCATION '/tmp/ecse/mv_046/'
AS SELECT i_brand, i_brand_id, i_manufact, i_manufact_id, SUM(ss_ext_sales_price) ext_price
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY i_brand, i_brand_id, i_manufact, i_manufact_id;

CREATE TABLE mv_047 USING CSV
LOCATION '/tmp/ecse/mv_047/'
AS SELECT c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year, COUNT(*) cnt, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / NULLIF(2, 0))) year_total
FROM catalog_sales
JOIN customer ON customer.c_customer_sk = catalog_sales.cs_c_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year;


CREATE TABLE mv_049 USING CSV
LOCATION '/tmp/ecse/mv_049/'
AS SELECT ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_wholesale_cost) sum_val
FROM store_sales
JOIN household_demographics ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city;

CREATE TABLE mv_051 USING CSV
LOCATION '/tmp/ecse/mv_051/'
AS SELECT i_brand, i_brand_id, i_manufact, i_manufact_id, s_store_id, s_store_name, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) fri_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) mon_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) sat_sales, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) sun_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) thu_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) wed_sales, SUM(ss_ext_sales_price) ext_price
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY i_brand, i_brand_id, i_manufact, i_manufact_id, s_store_id, s_store_name;

CREATE TABLE mv_052 USING CSV
LOCATION '/tmp/ecse/mv_052/'
AS SELECT i_item_id, AVG(ss_coupon_amt) agg3, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_sales_price) agg4
FROM store_sales
JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY i_item_id;

CREATE TABLE mv_053 USING CSV
LOCATION '/tmp/ecse/mv_053/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_id, s_store_name, s_zip, AVG(cs_quantity) catalog_sales_quantityave, AVG(cs_quantity) catalog_sales_quantitycov, AVG(sr_return_quantity) store_returns_quantityave, AVG(sr_return_quantity) store_returns_quantitycov, AVG(ss_coupon_amt) agg3, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_quantity) store_sales_quantityave, AVG(ss_quantity) store_sales_quantitycov, AVG(ss_sales_price) agg4, COUNT(*) cnt, COUNT(cs_quantity) catalog_sales_quantitycount, COUNT(sr_return_quantity) store_returns_quantitycount, COUNT(ss_quantity) store_sales_quantitycount, MIN(cs_net_profit) catalog_sales_profit, MIN(sr_net_loss) store_returns_loss, MIN(ss_net_profit) store_sales_profit, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_coupon_amt) s3, SUM(ss_ext_sales_price) gross_margin, SUM(ss_ext_sales_price) rank_within_parent, SUM(ss_ext_sales_price) sales, SUM(ss_list_price) s2, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_net_profit) gross_margin, SUM(ss_net_profit) netpaid, SUM(ss_net_profit) rank_within_parent, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_id, s_store_name, s_zip;

CREATE TABLE mv_054 USING CSV
LOCATION '/tmp/ecse/mv_054/'
AS SELECT AVG(ss_coupon_amt) agg3, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_quantity) avg_val, AVG(ss_sales_price) agg4, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk;

CREATE TABLE mv_055 USING CSV
LOCATION '/tmp/ecse/mv_055/'
AS SELECT ca_city, ca_zip, COUNT(*) cnt, SUM(ws_sales_price) sum_val
FROM web_sales
JOIN customer ON customer.c_customer_sk = web_sales.ws_c_sk
JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
GROUP BY ca_city, ca_zip;

CREATE TABLE mv_058 USING CSV
LOCATION '/tmp/ecse/mv_058/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_id, i_item_sk, i_product_name, s_store_id, s_store_name, s_zip, AVG(ss_coupon_amt) agg3, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_sales_price) agg4, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_coupon_amt) s3, SUM(ss_ext_sales_price) sales, SUM(ss_list_price) s2, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN promotion ON promotion.p_promo_sk = store_sales.ss_promo_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_id, i_item_sk, i_product_name, s_store_id, s_store_name, s_zip;

CREATE TABLE mv_059 USING CSV
LOCATION '/tmp/ecse/mv_059/'
AS SELECT i_item_id, s_store_id, AVG(ss_coupon_amt) agg3, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_sales_price) agg4, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN promotion ON promotion.p_promo_sk = store_sales.ss_promo_sk
GROUP BY i_item_id, s_store_id;

CREATE TABLE mv_060 USING CSV
LOCATION '/tmp/ecse/mv_060/'
AS SELECT i_item_id, AVG(ss_coupon_amt) agg3, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_quantity) avg_val, AVG(ss_sales_price) agg4, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY i_item_id;

CREATE TABLE mv_063 USING CSV
LOCATION '/tmp/ecse/mv_063/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_sk, i_product_name, s_store_id, s_store_name, s_zip, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_coupon_amt) s3, SUM(ss_ext_sales_price) sales, SUM(ss_list_price) s2, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN promotion ON promotion.p_promo_sk = store_sales.ss_promo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_sk, i_product_name, s_store_id, s_store_name, s_zip;

CREATE TABLE mv_064 USING CSV
LOCATION '/tmp/ecse/mv_064/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_name, s_zip, COUNT(*) cnt, SUM(ss_coupon_amt) s3, SUM(ss_list_price) s2, SUM(ss_net_profit) netpaid, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
JOIN store_returns ON store_returns.sr_store_returns_sk = store_sales.ss_sr_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_name, s_zip;

CREATE TABLE mv_065 USING CSV
LOCATION '/tmp/ecse/mv_065/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_sk, i_product_name, s_store_name, s_zip, COUNT(*) cnt, SUM(ss_coupon_amt) s3, SUM(ss_list_price) s2, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN promotion ON promotion.p_promo_sk = store_sales.ss_promo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year, i_item_sk, i_product_name, s_store_name, s_zip;

CREATE TABLE mv_066 USING CSV
LOCATION '/tmp/ecse/mv_066/'
AS SELECT c_first_name, c_last_name, ca_state, i_color, i_current_price, i_item_id, i_manager_id, i_manufact_id, i_size, i_units, s_state, s_store_name, COUNT(*) cnt, SUM(ss_ext_sales_price) total_sales, SUM(ss_net_profit) netpaid
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY c_first_name, c_last_name, ca_state, i_color, i_current_price, i_item_id, i_manager_id, i_manufact_id, i_size, i_units, s_state, s_store_name;

CREATE TABLE mv_067 USING CSV
LOCATION '/tmp/ecse/mv_067/'
AS SELECT ca_zip, i_item_id, i_manufact_id, AVG(TRY_CAST(c_birth_year AS DECIMAL(12, 2))) agg6, AVG(TRY_CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) agg7, AVG(TRY_CAST(cs_coupon_amt AS DECIMAL(12, 2))) agg3, AVG(TRY_CAST(cs_list_price AS DECIMAL(12, 2))) agg2, AVG(TRY_CAST(cs_net_profit AS DECIMAL(12, 2))) agg5, AVG(TRY_CAST(cs_quantity AS DECIMAL(12, 2))) agg1, AVG(TRY_CAST(cs_sales_price AS DECIMAL(12, 2))) agg4, SUM(cs_ext_sales_price) total_sales, SUM(cs_sales_price) sum_val
FROM catalog_sales
JOIN customer_address ON customer_address.ca_address_sk = catalog_sales.cs_addr_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY ca_zip, i_item_id, i_manufact_id;

CREATE TABLE mv_068 USING CSV
LOCATION '/tmp/ecse/mv_068/'
AS SELECT ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number;

CREATE TABLE mv_069 USING CSV
LOCATION '/tmp/ecse/mv_069/'
AS SELECT ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_wholesale_cost) sum_val
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN household_demographics ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, store.s_city;

CREATE TABLE mv_070 USING CSV
LOCATION '/tmp/ecse/mv_070/'
AS SELECT ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_wholesale_cost) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN household_demographics ON household_demographics.hd_demo_sk = store_sales.ss_hdemo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY ca_city, ss_addr_sk, ss_customer_sk, ss_ticket_number;

CREATE TABLE mv_071 USING CSV
LOCATION '/tmp/ecse/mv_071/'
AS SELECT c_first_name, c_last_name, ca_city, ca_state, i_color, i_current_price, i_manager_id, i_size, i_units, s_state, s_store_name, ss_addr_sk, ss_customer_sk, ss_ticket_number, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_net_profit) netpaid, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY c_first_name, c_last_name, ca_city, ca_state, i_color, i_current_price, i_manager_id, i_size, i_units, s_state, s_store_name, ss_addr_sk, ss_customer_sk, ss_ticket_number;



CREATE TABLE mv_076 USING CSV
LOCATION '/tmp/ecse/mv_076/'
AS SELECT cc_name, d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_current_price, i_item_desc, i_item_id, i_manager_id, i_manufact_id, s_company_name, s_store_name, AVG(SUM(cs_sales_price)) avg_monthly_sales, AVG(SUM(ss_sales_price)) avg_monthly_sales, COUNT(*) cnt, SUM(cs_sales_price) avg_monthly_sales, SUM(cs_sales_price) sum_sales, SUM(ss_sales_price) avg_monthly_sales, SUM(ss_sales_price) sum_sales
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY cc_name, d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_current_price, i_item_desc, i_item_id, i_manager_id, i_manufact_id, s_company_name, s_store_name;

CREATE TABLE mv_077 USING CSV
LOCATION '/tmp/ecse/mv_077/'
AS SELECT d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_current_price, i_item_desc, i_item_id, i_manager_id, i_manufact_id, s_company_name, s_store_name, AVG(SUM(ss_sales_price)) avg_monthly_sales, COUNT(*) cnt, SUM(ss_sales_price) avg_monthly_sales, SUM(ss_sales_price) sum_sales
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
GROUP BY d_moy, d_qoy, d_year, i_brand, i_category, i_class, i_current_price, i_item_desc, i_item_id, i_manager_id, i_manufact_id, s_company_name, s_store_name;

CREATE TABLE mv_078 USING CSV
LOCATION '/tmp/ecse/mv_078/'
AS SELECT cp_catalog_page_id, d1.d_week_seq, i_item_desc, i_item_id, w_state, w_warehouse_name, COUNT(*) cnt, COUNT(*) total_cnt, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) < TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_before, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) >= TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_after, SUM(CASE WHEN NOT p_promo_sk IS NULL THEN 1 ELSE 0 END) promo, SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo, SUM(COALESCE(cr_return_amount, 0)) returns, SUM(cs_ext_sales_price) sales, SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) profit
FROM catalog_sales
JOIN catalog_returns ON catalog_returns.cr_catalog_returns_sk = catalog_sales.cs_cr_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
GROUP BY cp_catalog_page_id, d1.d_week_seq, i_item_desc, i_item_id, w_state, w_warehouse_name;

CREATE TABLE mv_079 USING CSV
LOCATION '/tmp/ecse/mv_079/'
AS SELECT i_current_price, i_item_desc, i_item_id, COUNT(*) cnt
FROM inventory
JOIN date_dim ON date_dim.d_date_sk = inventory.inv_date_sk
JOIN item ON item.i_item_sk = inventory.inv_i_sk
GROUP BY i_current_price, i_item_desc, i_item_id;

CREATE TABLE mv_080 USING CSV
LOCATION '/tmp/ecse/mv_080/'
AS SELECT cc_name, d_moy, d_year, i_brand, i_category, i_current_price, i_item_desc, i_item_id, AVG(SUM(cs_sales_price)) avg_monthly_sales, COUNT(*) cnt, SUM(cs_sales_price) avg_monthly_sales, SUM(cs_sales_price) sum_sales
FROM inventory
JOIN catalog_sales ON catalog_sales.cs_catalog_sales_sk = inventory.inv_cs_sk
JOIN date_dim ON date_dim.d_date_sk = inventory.inv_date_sk
JOIN item ON item.i_item_sk = inventory.inv_i_sk
GROUP BY cc_name, d_moy, d_year, i_brand, i_category, i_current_price, i_item_desc, i_item_id;


CREATE TABLE mv_085 USING CSV
LOCATION '/tmp/ecse/mv_085/'
AS SELECT ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_id, s_store_name, s_zip, AVG(cs_quantity) catalog_sales_quantityave, AVG(cs_quantity) catalog_sales_quantitycov, AVG(sr_return_quantity) store_returns_quantityave, AVG(sr_return_quantity) store_returns_quantitycov, AVG(ss_quantity) store_sales_quantityave, AVG(ss_quantity) store_sales_quantitycov, COUNT(*) cnt, COUNT(cs_quantity) catalog_sales_quantitycount, COUNT(sr_return_quantity) store_returns_quantitycount, COUNT(ss_quantity) store_sales_quantitycount, MIN(cs_net_profit) catalog_sales_profit, MIN(sr_net_loss) store_returns_loss, MIN(ss_net_profit) store_sales_profit, SUM(ss_coupon_amt) s3, SUM(ss_list_price) s2, SUM(ss_net_profit) netpaid, SUM(ss_wholesale_cost) s1
FROM store_sales
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
JOIN store_returns ON store_returns.sr_store_returns_sk = store_sales.ss_sr_sk
GROUP BY ad1.ca_city, ad1.ca_street_name, ad1.ca_street_number, ad1.ca_zip, ad2.ca_city, ad2.ca_street_name, ad2.ca_street_number, ad2.ca_zip, c_first_name, c_last_name, ca_state, d1.d_year, d2.d_year, d3.d_year, i_color, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manager_id, i_product_name, i_size, i_units, s_state, s_store_id, s_store_name, s_zip;

CREATE TABLE mv_086 USING CSV
LOCATION '/tmp/ecse/mv_086/'
AS SELECT c_first_name, c_last_name, ca_city, ca_county, ca_state, d_qoy, d_year, i_color, i_current_price, i_item_id, i_manager_id, i_manufact_id, i_size, i_units, s_state, s_store_name, ss_addr_sk, ss_customer_sk, ss_ticket_number, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_quantity) avg_val, COUNT(*) cnt, SUM(ss_ext_sales_price) store_sales, SUM(ss_ext_sales_price) total_sales, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_net_profit) netpaid, SUM(ss_quantity) sum_val
FROM store_sales
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
GROUP BY c_first_name, c_last_name, ca_city, ca_county, ca_state, d_qoy, d_year, i_color, i_current_price, i_item_id, i_manager_id, i_manufact_id, i_size, i_units, s_state, s_store_name, ss_addr_sk, ss_customer_sk, ss_ticket_number;

CREATE TABLE mv_087 USING CSV
LOCATION '/tmp/ecse/mv_087/'
AS SELECT c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year, COUNT(*) cnt, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / NULLIF(2, 0))) year_total, SUM((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / NULLIF(2, 0))) year_total, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / NULLIF(2, 0)) year_total, SUM(ss_ext_list_price - ss_ext_discount_amt) year_total, SUM(ws_ext_list_price - ws_ext_discount_amt) year_total
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year;

CREATE TABLE mv_088 USING CSV
LOCATION '/tmp/ecse/mv_088/'
AS SELECT ca_zip, AVG(TRY_CAST(c_birth_year AS DECIMAL(12, 2))) agg6, AVG(TRY_CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) agg7, AVG(TRY_CAST(cs_coupon_amt AS DECIMAL(12, 2))) agg3, AVG(TRY_CAST(cs_list_price AS DECIMAL(12, 2))) agg2, AVG(TRY_CAST(cs_net_profit AS DECIMAL(12, 2))) agg5, AVG(TRY_CAST(cs_quantity AS DECIMAL(12, 2))) agg1, AVG(TRY_CAST(cs_sales_price AS DECIMAL(12, 2))) agg4, COUNT(*) cnt, SUM(cs_sales_price) sum_val
FROM catalog_sales
JOIN customer ON customer.c_customer_sk = catalog_sales.cs_c_sk
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
GROUP BY ca_zip;

CREATE TABLE mv_089 USING CSV
LOCATION '/tmp/ecse/mv_089/'
AS SELECT s_store_id, COUNT(*) cnt, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(ss_ext_sales_price) sales, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN promotion ON promotion.p_promo_sk = store_sales.ss_promo_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY s_store_id;

CREATE TABLE mv_090 USING CSV
LOCATION '/tmp/ecse/mv_090/'
AS SELECT cp_catalog_page_id, i_item_id, AVG(cs_coupon_amt) agg3, AVG(cs_list_price) agg2, AVG(cs_quantity) agg1, AVG(cs_sales_price) agg4, SUM(COALESCE(cr_return_amount, 0)) returns, SUM(cs_ext_sales_price) sales, SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) profit
FROM catalog_sales
JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
JOIN promotion ON promotion.p_promo_sk = catalog_sales.cs_promo_sk
GROUP BY cp_catalog_page_id, i_item_id;


CREATE TABLE mv_092 USING CSV
LOCATION '/tmp/ecse/mv_092/'
AS SELECT ca_state, i_item_id, wp_web_page_sk, wr_returning_customer_sk, SUM(wr_net_loss) profit_loss, SUM(wr_return_amt) ctr_total_return, SUM(wr_return_amt) returns, SUM(wr_return_quantity) wr_item_qty
FROM web_returns
JOIN date_dim ON date_dim.d_date_sk = web_returns.wr_sold_date_sk
GROUP BY ca_state, i_item_id, wp_web_page_sk, wr_returning_customer_sk;

CREATE TABLE mv_093 USING CSV
LOCATION '/tmp/ecse/mv_093/'
AS SELECT d1.d_week_seq, i_item_desc, i_item_id, w_warehouse_name, AVG(cs_coupon_amt) agg3, AVG(cs_list_price) agg2, AVG(cs_quantity) agg1, AVG(cs_sales_price) agg4, COUNT(*) total_cnt, SUM(CASE WHEN NOT p_promo_sk IS NULL THEN 1 ELSE 0 END) promo, SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo
FROM catalog_sales
JOIN customer_demographics ON customer_demographics.cd_demo_sk = catalog_sales.cs_cdemo_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
GROUP BY d1.d_week_seq, i_item_desc, i_item_id, w_warehouse_name;

CREATE TABLE mv_094 USING CSV
LOCATION '/tmp/ecse/mv_094/'
AS SELECT d1.d_week_seq, i_item_desc, i_item_id, w_state, w_warehouse_name, COUNT(*) total_cnt, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) < TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_before, SUM(CASE WHEN (TRY_CAST(d_date AS DATE) >= TRY_CAST('2002-05-18' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) sales_after, SUM(CASE WHEN NOT p_promo_sk IS NULL THEN 1 ELSE 0 END) promo, SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo
FROM catalog_sales
JOIN catalog_returns ON catalog_returns.cr_catalog_returns_sk = catalog_sales.cs_cr_sk
JOIN item ON item.i_item_sk = catalog_sales.cs_i_sk
JOIN warehouse ON warehouse.w_warehouse_sk = catalog_sales.cs_warehouse_sk
GROUP BY d1.d_week_seq, i_item_desc, i_item_id, w_state, w_warehouse_name;

CREATE TABLE mv_095 USING CSV
LOCATION '/tmp/ecse/mv_095/'
AS SELECT c_first_name, c_last_name, ca_state, i_color, i_current_price, i_manager_id, i_size, i_units, s_state, s_store_name, COUNT(*) cnt, SUM(ss_net_profit) netpaid
FROM store_sales
JOIN customer ON customer.c_customer_sk = store_sales.ss_c_sk
JOIN customer_address ON customer_address.ca_address_sk = store_sales.ss_addr_sk
JOIN item ON item.i_item_sk = store_sales.ss_i_sk
JOIN store ON store.s_store_sk = store_sales.ss_s_sk
GROUP BY c_first_name, c_last_name, ca_state, i_color, i_current_price, i_manager_id, i_size, i_units, s_state, s_store_name;

CREATE TABLE mv_096 USING CSV
LOCATION '/tmp/ecse/mv_096/'
AS SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
FROM web_sales
UNION ALL
SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
FROM catalog_sales;

CREATE TABLE mv_097 USING CSV
LOCATION '/tmp/ecse/mv_097/'
AS SELECT date_dim.d_week_seq,
       SUM(CASE WHEN (date_dim.d_day_name = 'Sunday') THEN mv_096.sales_price ELSE NULL END) sun_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Monday') THEN mv_096.sales_price ELSE NULL END) mon_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Tuesday') THEN mv_096.sales_price ELSE NULL END) tue_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Wednesday') THEN mv_096.sales_price ELSE NULL END) wed_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Thursday') THEN mv_096.sales_price ELSE NULL END) thu_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Friday') THEN mv_096.sales_price ELSE NULL END) fri_sales,
       SUM(CASE WHEN (date_dim.d_day_name = 'Saturday') THEN mv_096.sales_price ELSE NULL END) sat_sales
FROM mv_096
JOIN date_dim ON date_dim.d_date_sk = mv_096.sold_date_sk
GROUP BY date_dim.d_week_seq;

CREATE TABLE mv_098 USING CSV
LOCATION '/tmp/ecse/mv_098/'
AS SELECT mv_097.d_week_seq, date_dim.d_year,
       mv_097.sun_sales, mv_097.mon_sales, mv_097.tue_sales, mv_097.wed_sales,
       mv_097.thu_sales, mv_097.fri_sales, mv_097.sat_sales
FROM mv_097
JOIN date_dim ON date_dim.d_week_seq = mv_097.d_week_seq;

CREATE TABLE mv_099 USING CSV
LOCATION '/tmp/ecse/mv_099/'
AS SELECT dt.d_year,
          item.i_brand_id brand_id,
          item.i_brand brand,
          SUM(store_sales.ss_ext_sales_price) sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 436
  AND dt.d_moy = 12
GROUP BY dt.d_year, item.i_brand, item.i_brand_id;

CREATE TABLE mv_100 USING CSV
LOCATION '/tmp/ecse/mv_100/'
AS SELECT dt.d_year,
          dt.d_moy,
          item.i_manufact_id,
          item.i_brand_id brand_id,
          item.i_brand brand,
          SUM(store_sales.ss_ext_sales_price) sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY dt.d_year, dt.d_moy, item.i_manufact_id, item.i_brand, item.i_brand_id;

CREATE TABLE mv_101 USING CSV
LOCATION '/tmp/ecse/mv_101/'
AS SELECT dt.d_year,
          dt.d_moy,
          item.i_manufact_id,
          item.i_brand_id,
          item.i_brand as kk,
          store_sales.ss_ext_sales_price as miaomiao
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk;

CREATE TABLE mv_102 USING CSV
LOCATION '/tmp/ecse/mv_102/'
AS SELECT c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year,
       SUM(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
FROM customer, store_sales, date_dim
WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year;

CREATE TABLE mv_103 USING CSV
LOCATION '/tmp/ecse/mv_103/'
AS SELECT c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year,
       SUM((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
FROM customer, catalog_sales, date_dim
WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk
GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year;

CREATE TABLE mv_104 USING CSV
LOCATION '/tmp/ecse/mv_104/'
AS SELECT c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year,
       SUM((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
FROM customer, web_sales, date_dim
WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year;


CREATE TABLE v1 USING CSV
LOCATION '/tmp/ecse/v1/'
AS SELECT 
    i_category,
    AVG(i_current_price) AS avg_price,
    MIN(i_current_price) AS min_price,
    MAX(i_current_price) AS max_price,
    COUNT(*) AS item_count
FROM item
GROUP BY i_category;

CREATE TABLE v2 USING CSV
LOCATION '/tmp/ecse/v2/'
AS SELECT 
    ca.ca_state,
    d.d_month_seq,
    i.i_category,
    i.i_current_price,
    ss.ss_item_sk,
    COUNT(*) AS monthly_cnt,
    SUM(ss.ss_ext_sales_price) AS monthly_sales
FROM store_sales ss
JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk
JOIN customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
JOIN item i ON i.i_item_sk = ss.ss_item_sk
GROUP BY ca.ca_state, d.d_month_seq, i.i_category, i.i_current_price, ss.ss_item_sk;

CREATE TABLE v3 USING CSV
LOCATION '/tmp/ecse/v3/'
AS SELECT 
    ca_state,
    COUNT(*) AS total_cnt,
    SUM(CASE WHEN d_month_seq = 200002 THEN 1 ELSE 0 END) AS feb_2000_cnt,
    MIN(i_current_price) AS min_price,
    MAX(i_current_price) AS max_price,
    AVG(i_current_price) AS avg_price
FROM v2
GROUP BY ca_state
HAVING COUNT(*) >= 5;  

CREATE TABLE v6_month_seq USING CSV
LOCATION '/tmp/ecse/v6_month_seq/'
AS SELECT DISTINCT d_month_seq
FROM date_dim
WHERE d_year = 2000
  AND d_moy = 2;

CREATE TABLE v6_item_avg USING CSV
LOCATION '/tmp/ecse/v6_item_avg/'
AS SELECT i_category,
          AVG(i_current_price) AS avg_price
FROM item
GROUP BY i_category;

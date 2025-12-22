from sqlglot import parse_one
from mv_transfer import mv_transfer
from ViewMatcher import view_match
from tpc_query import Colors
#sql_view=[]
sql_query=[]
sql_view={"view1":"""
AS SELECT c_customer_sk, ca_city, d_month_seq,ca_county, ca_state, cd_credit_rating, cd_dep_college_count, cd_dep_count, cd_dep_employed_count, cd_education_status, cd_gender, cd_marital_status, cd_purchase_estimate, d_week_seq, d_year, i_brand, i_brand_id, i_category, i_category_id, i_class, i_class_id, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manufact_id, s_store_id, s_store_name, s_store_sk, ss_addr_sk, ss_customer_sk, ss_item_sk, ss_store_sk, ss_ticket_number, store.s_city, t_hour, t_minute, AVG(ss_coupon_amt) agg3, AVG(ss_ext_sales_price) avg_val, AVG(ss_ext_wholesale_cost) avg_val, AVG(ss_list_price) agg2, AVG(ss_quantity) agg1, AVG(ss_quantity) avg_val, AVG(ss_sales_price) agg4, COUNT(*) cnt, COUNT(*) cnt1, COUNT(*) cnt2, COUNT(*) cnt3, COUNT(*) cnt4, COUNT(*) cnt5, COUNT(*) cnt6, COUNT(*) number_sales, COUNT(*) sales_cnt, MAX(cd_dep_college_count) max_val, MAX(cd_dep_count) max_val, MAX(cd_dep_employed_count) max_val, MAX(csales) tpcds_cmax, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) fri_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) mon_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) sat_sales, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) sun_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) thu_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) wed_sales, SUM(COALESCE(sr_return_amt, 0)) returns, SUM(SUM(ss_ext_sales_price)) revenueratio, SUM(SUM(ss_sales_price)) cume_sales, SUM(ext_price) ext_price, SUM(ext_sales_price) sales_amt, SUM(ss_ext_sales_price) itemrevenue, SUM(ss_ext_sales_price) revenueratio, SUM(ss_ext_sales_price) sales, SUM(ss_ext_sales_price) ss_item_rev, SUM(ss_ext_sales_price) store_sales, SUM(ss_ext_sales_price) total_sales, SUM(ss_ext_wholesale_cost) sum_val, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) profit, SUM(ss_net_profit) profit, SUM(ss_net_profit) rank_within_parent, SUM(ss_net_profit) sum_val, SUM(ss_net_profit) total_sum, SUM(ss_quantity * ss_list_price) sales, SUM(ss_quantity) ss_qty, SUM(ss_quantity) sum_val, SUM(ss_sales_price) cume_sales, SUM(ss_sales_price) ss_sp, SUM(ss_wholesale_cost) ss_wc
FROM store_sales
JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
GROUP BY c_customer_sk, ca_city, ca_county, ca_state, cd_credit_rating, cd_dep_college_count, cd_dep_count, cd_dep_employed_count, cd_education_status, cd_gender, cd_marital_status, cd_purchase_estimate, d_week_seq, d_year, i_brand, i_brand_id, i_category, i_category_id, i_class, i_class_id, i_current_price, i_item_desc, i_item_id, i_item_sk, i_manufact_id, s_store_id, s_store_name, s_store_sk, ss_addr_sk, ss_customer_sk, ss_item_sk, ss_store_sk, ss_ticket_number, store.s_city, t_hour, t_minute;

"""}
sql_view=mv_transfer(r"D:\wechatdocuments\xwechat_files\qweasd1578256388_a398\msg\file\2025-11\m1_ddl.sql")
sql_query.append("""
-- start query 1 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1212 and 1223
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1212 and 1212 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;

-- end query 1 in stream 0 using template query97.tpl


""")
#print('\033[89m',repr(parse_one(sql_view[1])),'\033[0m')
#print(repr(parse_one(sql_query[0])))
for i,query in enumerate(sql_query):
    if i==1:
        break
    print(f"====== start {i+1} ======")
    new_query_sql=view_match(query,sql_view)
    print(f"{Colors.MAGENTA}{Colors.BOLD}NEW SQL:\n", new_query_sql)
    '''
    flag,comp1,comp2,c3,sel,rewrite_map,new_query_sql=view_match(sql_query[i],view)
    print(f"{Colors.GREEN}result:",flag)
    print(f"{Colors.YELLOW}compensation1:",comp1)
    print(f"{Colors.YELLOW}compensation2:",comp2)
    print(c3)
    print(f"{Colors.BLUE}spj_select_change:",sel)
    print(f"{Colors.WHITE}agg_rewrite_map:",rewrite_map)
    print(f"{Colors.MAGENTA}new_query_sql:",new_query_sql)
    '''
    print(f"{Colors.END}======  end {i+1}  ======\n\n\n")


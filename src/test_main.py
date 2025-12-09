from sqlglot import parse_one
from mv_transfer import mv_transfer
from ViewMatcher import view_match
from tpc_query import Colors
#sql_view=[]
sql_query=[]
'''
sql_view.append("""
select 'a'||dv_version as abcd,round(dv_version/dv_create_date,2) as jkl,'dd' as qwe
from dbgen_version
where dv_version>=0
group by rollup(dv_version,dv_create_date);
""")
sql_view.append("""
select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs 
     ,date_dim
 where d_date_sk = ws_sold_date_sk
 group by d_week_seq
        """)
'''
sql_view=mv_transfer(r"D:\wechatdocuments\xwechat_files\qweasd1578256388_a398\msg\file\2025-11\m1_ddl.sql")
sql_query.append("""
-- start query 1 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1212 and 1212 + 11
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


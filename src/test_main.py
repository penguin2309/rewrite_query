from sqlglot import parse_one
from mv_transfer import mv_transfer
from ViewMatcher import view_match
from tpc_query_main import Colors
from config import VIEW_DDL_FILE
#sql_view=[]
sql_query=[]
sql_view={"view1":"""
SELECT 
    i_item_id, 
    d_year,
    s_store_sk, 
    sr_customer_sk, 
    sr_store_sk, 
    SUM(SR_RETURN_AMT_INC_TAX) ctr_total_return,
    SUM(sr_net_loss) profit_loss,
    SUM(sr_return_amt) returns,
    SUM(sr_return_quantity) sr_item_qty,
    SUM(sr_fee) tot_returns,  -- 对sr_fee求和
    COUNT_BIG(*) cnt_big
FROM store_returns
JOIN date_dim ON date_dim.d_date_sk = store_returns.sr_returned_date_sk
GROUP BY 
    i_item_id, 
    s_store_sk, 
    sr_customer_sk, 
    sr_store_sk,
    d_year
"""}
sql_view=mv_transfer(VIEW_DDL_FILE)
sql_query.append("""
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales 
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as 
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim 
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,date_dim 
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 2001+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1;

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


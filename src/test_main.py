from sqlglot import parse_one

from ViewMatcher import view_match
from tpc_query import Colors
sql_view=[]
sql_query=[]

sql_view.append("""
select dv_version,round(dv_version/dv_create_date,2) as jkl,'dd' as qwe
from dbgen_version;
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
sql_query.append("""
select dv_version,round(dv_version/dv_create_date,2) as rfd

from dbgen_version where dv_version=203+1 and dv_version between 1 and 2;
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

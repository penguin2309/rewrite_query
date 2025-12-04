from sqlglot import parse_one

from ViewMatcher import view_match
from tpc_query import Colors
sql_view=[]
sql_query=[]

sql_view.append("""
select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales 
""")

sql_query.append("""
select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales 
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales
        union 
        select m from mm;
""")

#print(repr(parse_one(sql_query[0])))
for i,query in enumerate(sql_query):
    if i==1:
        break
    print(f"====== start {i+1} ======")
    new_query_sql=view_match(query,sql_view)
    print(f"{Colors.MAGENTA}{Colors.BOLD},new_query_sql:\n", new_query_sql)
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

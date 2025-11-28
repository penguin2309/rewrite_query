from ViewMatcher import view_match
from tpc_query import Colors
sql_view=[]
sql_query=[]

sql_view.append("""
select  i_brand_id brand_id_VIEW, i_brand brand_VIEW,
d_date_sk,ss_sold_date_sk,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where ss_item_sk = i_item_sk
 	and i_manager_id=36
 	and d_moy=12
 	and d_year=2001
 group by i_brand, i_brand_id;
""")

sql_query.append("""
select  i_brand_id brand_id_q, i_brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=36
 	and d_moy=12
 	and d_year=2001
 group by i_brand
 order by ext_price desc, i_brand_id
limit 100 ;
""")
sql_view.append("""
SELECT 
    region,
    A.category,
    A.id,
    A.sub_category,
    COUNT_BIG(*) as cnt_big,
    SUM(B.quantity) as total_quantity
FROM A, B
WHERE A.id = B.a_id
GROUP BY A.region, A.category,A.id;
""")
sql_query.append("""
SELECT 
    A.region,
    COUNT(*) as order_count,
    SUM(B.quantity) as total_quantity,
    AVG(B.quantity) as avg_quantity
FROM A, B
WHERE A.id = B.a_id
AND A.region>B.a_id
AND A.category = 2
AND A.sub_category=1
GROUP BY A.region;
""")

sql_view.append("""
SELECT 
    A.region,
    A.category, 
    B.quantity,
    E.value
FROM A, B, E
WHERE A.id = B.a_id
AND B.a_id = E.b_id
AND A.category BETWEEN 1 AND 5;
""")
sql_query.append("""
SELECT 
    A.region,
    SUM(B.quantity) as total_qty
FROM A, B
WHERE A.id = B.a_id  
AND A.category = 3
GROUP BY A.region;
""")

sql_view.append("""
SELECT 
    A.id,
    A.region,
    A.category,
    B.quantity,
    B.price
FROM A, B
WHERE A.id = B.a_id
AND A.category > 0.9
AND A.region LIKE 'North%';
""")
sql_query.append("""
SELECT 
    A.region As rg,
    B.quantity As qty,
    B.a_id As id
FROM A, B  
WHERE A.id = B.a_id
AND A.category = 1
AND B.price=12333
AND B.quantity>90
AND B.quantity<90.09
AND A.region LIKE 'North%'
AND B.quantity > 100;
""")

sql_view.append("""
SELECT 
    A.region,
    A.category,
    COUNT_BIG(*) AS cnt_big,
    SUM(B.quantity) AS total_quantity,
    SUM(A.quantity ) AS total_A,
    B.xx,
    A.yy
FROM A,B
WHERE A.id = B.a_id
GROUP BY A.region, A.category,B.xx,A.yy;
""")
sql_query.append("""
SELECT 
    A.region,
    COUNT(*) AS order_count,
    SUM(B.quantity) AS total_quantity,
    AVG(B.quantity ) AS avg
FROM A,B
WHERE A.id = B.a_id
AND B.xx=A.yy
AND A.category = 1
GROUP BY A.region;
""")
# 预期：匹配成功
# 预期补偿表达式：
# SELECT region,
#        SUM(cnt_big) AS order_count,
#        SUM(total_quantity) AS total_quantity,
#        SUM(total_revenue) / SUM(cnt_big) AS avg_revenue
# FROM view
# WHERE category = 'Electronics'
# GROUP BY region

sql_view.append("""
SELECT 
    Customers.category,
    COUNT_BIG(*) AS cnt_big,
    Orders.id AS order_id,
    SUM(Orders.amount) AS total_sales
FROM Customers,Orders
WHERE Customers.id = Orders.customer_id
GROUP BY Customers.category,Orders.id;
""")
sql_query.append("""
SELECT 
    Customers.category,
    SUM(Orders.amount) AS total_sales,
    AVG(Orders.amount) AS avg_sales
FROM Customers,Orders
WHERE Customers.id = Orders.customer_id
GROUP BY Customers.category;
""")

sql_view.append("""
SELECT A.id, B.a_id, C.id, D.c_id, E.b_id
FROM   A,B,C,D,E
WHERE B.a_id = A.id
AND E.b_id = B.a_id
AND D.c_id = C.id;
""")
sql_query.append("""
SELECT A.id,C.id
FROM   A,C
""")

sql_view.append("""
SELECT A.id, B.a_id, C.a_id, D.c_id
FROM   A,B,C,D
WHERE B.a_id = A.id
and C.a_id = A.id
and D.c_id = C.id;
""")
sql_query.append("""
SELECT A.id FROM A WHERE A.id>10
""")

sql_view.append("""
SELECT A.x,A.id,B.a_id,C.b_id FROM A,B,C WHERE A.id = B.a_id and B.a_id=C.b_id
""")
sql_query.append("""
SELECT A.x FROM A WHERE A.x>10
""")

sql_view.append("""
SELECT 
    orders.o_orderkey,
    orders.o_custkey,
    orders.o_orderdate, 
    lineitem.l_partkey,
    lineitem.l_quantity,
    lineitem.l_extendedprice,
    lineitem.l_shipdate, 
    part.p_name
FROM orders, lineitem
WHERE 
    orders.o_orderkey = lineitem.l_orderkey
    AND lineitem.l_partkey = part.p_partkey
    AND orders.o_custkey >=50 
    AND orders.o_custkey<=500
    AND lineitem.l_quantity >= 1
    AND part.p_name LIKE '%metal%';
""")
sql_query.append("""
SELECT 
    lineitem.l_orderkey AS order_id
FROM orders, lineitem, part
WHERE 
    orders.o_orderkey = lineitem.l_orderkey
    AND lineitem.l_partkey = part.p_partkey
    AND orders.o_custkey = 123
    AND lineitem.l_quantity>=10 
    AND lineitem.l_quantity<=100
    AND lineitem.l_extendedprice > 1000
    AND orders.o_orderdate = lineitem.l_shipdate;
""")

sql_view.append("""
SELECT orders.o_orderkey, orders.o_totalprice, orders.o_custkey, customer.c_name, customer.c_phone
FROM orders, customer
WHERE orders.o_custkey = customer.c_custkey 
""")
sql_query.append("""
SELECT orders.o_orderkey, orders.o_totalprice 
FROM orders
WHERE orders.o_orderdate >= '2024-01-01'
""")

for i,view in enumerate(sql_view):
    if i>0:
        break
    print(f"====== start {i+1} ======")
    flag,comp1,comp2,c3,sel,rewrite_map,new_query_sql=view_match(sql_query[i],view,True)
    print(f"{Colors.GREEN}result:",flag)
    print(f"{Colors.YELLOW}compensation1:",comp1)
    print(f"{Colors.YELLOW}compensation2:",comp2)
    print(c3)
    print(f"{Colors.BLUE}spj_select_change:",sel)
    print(f"{Colors.WHITE}agg_rewrite_map:",rewrite_map)
    print(f"{Colors.MAGENTA}new_query_sql:",new_query_sql)
    print(f"{Colors.END}======  end {i+1}  ======\n\n\n")

import os
os.environ['HADOOP_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['HADOOP_COMMON_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['HADOOP_HDFS_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}\\bin;{os.environ['HADOOP_HOME']}\\bin;{os.environ['PATH']}"
os.environ['PYSPARK_PYTHON'] = r'C:\Users\o2309\PycharmProjects\PythonProject1\.venv\Scripts\python.exe'
from sqlglot import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns
import tpc_read as r
#from ViewMatcher import view_match
class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def main():
    spark = SparkSession.builder \
        .appName("tpc_query") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ui.port", "2025") .config("spark.ui.enableHint", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    call_center, customer_address, customer_demographics, \
        date_dim, warehouse, ship_mode, time_dim, reason, income_band, \
        item, store, customer, web_site, store_returns, household_demographics, \
        web_page, promotion, catalog_page, inventory, catalog_returns, web_returns, \
        web_sales, catalog_sales, store_sales=r.read(spark)

    #call_center.printSchema()
    #call_center.show(5, truncate=False)
    #call_center.createOrReplaceTempView("call_center")
    #catalog_page.show(5, truncate=False)
    #res=spark.sql("select * from call_center")
    start=int(input("start:"))
    end=int(input("end:"))
    path=r"\\wsl.localhost\Ubuntu\home\o2309\tpcds-kit\tools\tpcds-query"
    for i in range(start,end+1):
        file_path=os.path.join(path,f"query_{i}.sql")
        print(file_path)
        print("start ",i)
        with open(file_path) as f:
            s=f.read()

        #tree=parse(s)
        #print(f"{Colors.GREEN}")
        #print(tree)
        #print(f"{Colors.END}")
        view_sql="""
        select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=36
 	and d_moy=12
 	and d_year=2001
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
limit 100 ;
        """
        s=view_match(s,view_sql)
        res=spark.sql(s)
        res.cache()
        res.show()
        count=res.count()
        print(f"{Colors.YELLOW}")
        print(f"返回 {count} 条数据")
        print(f"{Colors.END}")
        print(f"{Colors.MAGENTA}")
        res.explain(mode="extended")
        print(f"{Colors.END}")
        print("end ",i)

    input("press enter to exit")
    spark.stop()
if __name__=="__main__":
    main()
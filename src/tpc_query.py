import os
import time
os.environ['HADOOP_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['HADOOP_COMMON_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['HADOOP_HDFS_HOME'] = r'D:\code\hadoop-3.3.6'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}\\bin;{os.environ['HADOOP_HOME']}\\bin;{os.environ['PATH']}"
os.environ['PYSPARK_PYTHON'] = r'C:\Users\o2309\PycharmProjects\PythonProject1\.venv\Scripts\python.exe'
from sqlglot import parse, expressions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns
import tpc_read as r
from mv_transfer import mv_transfer
from ViewMatcher import view_match
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

def load_mv_ddls(file_path):
    with open(file_path) as f:
        ddl_sql = f.read()
    parsed = parse(ddl_sql)
    ddl_map = {}
    for stmt in parsed:
        if isinstance(stmt, expressions.Create):
            name = stmt.this.this.this
            ddl_map[name] = stmt.sql(dialect="spark")
    return ddl_map

def prompt_views(ddl_map):
    names = list(ddl_map.keys())
    print(f"可用视图数量: {len(names)}")
    print(",".join(names))
    raw = input("请输入要创建的视图名称，逗号分隔，或all:")
    raw = raw.strip()
    if raw.lower() == "all":
        return names
    selected = []
    for item in raw.split(","):
        name = item.strip()
        if not name:
            continue
        if name in ddl_map:
            selected.append(name)
    return selected

def align_df(df, target_cols):
    col_map = {c.lower(): c for c in df.columns}
    select_cols = []
    for c in target_cols:
        select_cols.append(col(col_map[c.lower()]).alias(c))
    return df.select(*select_cols)

def compare_results(df1, df2):
    cols1 = [c.lower() for c in df1.columns]
    cols2 = [c.lower() for c in df2.columns]
    if set(cols1) != set(cols2):
        return False, "列集合不一致"
    df2_aligned = align_df(df2, df1.columns)
    diff1 = df1.exceptAll(df2_aligned).count()
    diff2 = df2_aligned.exceptAll(df1).count()
    if diff1 == 0 and diff2 == 0:
        return True, "结果一致"
    return False, f"结果不一致: 差异行数 {diff1 + diff2}"

def main():
    spark = SparkSession.builder \
        .appName("tpc_query") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
        .config("spark.ui.port", "2025") .config("spark.ui.enableHint", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    call_center, customer_address, customer_demographics, \
        date_dim, warehouse, ship_mode, time_dim, reason, income_band, \
        item, store, customer, web_site, store_returns, household_demographics, \
        web_page, promotion, catalog_page, inventory, catalog_returns, web_returns, \
        web_sales, catalog_sales, store_sales=r.read(spark)

    ddl_path = r"C:\Users\o2309\PycharmProjects\PythonProject1\view.sql"
    view_sqls = mv_transfer(ddl_path)
    ddl_map = load_mv_ddls(ddl_path)
    selected_views = prompt_views(ddl_map)
    if selected_views:
        for name in selected_views:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
            spark.sql(ddl_map[name])
        print(f"{Colors.GREEN}已创建 {len(selected_views)} 个物化视图{Colors.END}")
    else:
        print(f"{Colors.YELLOW}未创建任何物化视图{Colors.END}")
    selected_view_sqls = {name: view_sqls[name] for name in selected_views if name in view_sqls}

    start=int(input("start:"))
    end=int(input("end:"))
    path=r"\\wsl.localhost\Ubuntu-24.04\home\o2309\tpcds-kit\tools\tpcds-query"
    for i in range(start,end+1):
        file_path=os.path.join(path,f"query_{i}.sql")
        print(file_path)
        print("start ",i)
        with open(file_path) as f:
            s=f.read()

        try:
            rewritten_sql = view_match(s, selected_view_sqls) if selected_view_sqls else s
        except Exception as e:
            print(f"{Colors.RED}视图重写失败: {e}{Colors.END}")
            rewritten_sql = s

        print(f"{Colors.BLUE}执行原始查询{Colors.END}")
        raw_start = time.perf_counter()
        raw_df = spark.sql(s)
        raw_df.cache()
        raw_df.show()
        raw_count = raw_df.count()
        raw_end = time.perf_counter()
        raw_elapsed = raw_end - raw_start
        print(f"{Colors.YELLOW}原始查询返回 {raw_count} 条数据, 耗时 {raw_elapsed:.3f} 秒{Colors.END}")

        print(f"{Colors.BLUE}执行视图重写查询{Colors.END}")
        print(f"{rewritten_sql}")
        rewrite_start = time.perf_counter()
        rewrite_df = spark.sql(rewritten_sql)
        rewrite_df.cache()
        rewrite_df.show()
        rewrite_count = rewrite_df.count()
        rewrite_end = time.perf_counter()
        rewrite_elapsed = rewrite_end - rewrite_start
        print(f"{Colors.YELLOW}重写查询返回 {rewrite_count} 条数据, 耗时 {rewrite_elapsed:.3f} 秒{Colors.END}")

        print(f"{Colors.CYAN}查询 {i} 原始/重写耗时: {raw_elapsed:.3f}s / {rewrite_elapsed:.3f}s{Colors.END}")

        same, msg = compare_results(raw_df, rewrite_df)
        color = Colors.GREEN if same else Colors.RED
        print(f"{color}{msg}{Colors.END}")
        print("end ",i)

    input("press enter to exit")
    spark.stop()
if __name__=="__main__":
    main()

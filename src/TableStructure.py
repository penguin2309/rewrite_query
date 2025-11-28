from SPJGExpression import *
from sqlglot import expressions as exp
import sqlglot
from tpc_schema import sqls,tables_name,primary_keys
class TableStructure:
    def __init__(self,name):
        self.name = name
        self.columns = {}
        self.primary_key=[]
        self.unique_keys=[]
        self.foreign_keys=[]

    def add_column(self, name, is_nullable=True):
        self.columns[name] = ColumnInfo(name, is_nullable)

    def set_primary_key(self, columns):
        columns=[columns]
        self.primary_key = set(columns)
        for col in columns:
            if col in self.columns:
                self.columns[col].is_nullable = False
            if col not in self.unique_keys:
                self.unique_keys.append(col)

    def add_unique_key(self, col,is_nullable=True):
        self.unique_keys.append(col)

    def add_foreign_key(self, local_columns, ref_table, ref_columns,is_nullable=True):
        self.foreign_keys.append(ForeignKeyConstraint(
            local_columns, ref_table, ref_columns,is_nullable
        ))

class ColumnInfo:
    def __init__(self, name, is_nullable=True):
        self.name = name
        self.is_nullable = is_nullable

class ForeignKeyConstraint:
    def __init__(self, local_columns, ref_table, ref_columns,is_nullable=True):
        self.local_columns = local_columns  # 本表的外键列
        self.ref_table = ref_table  # 被引用的表名
        self.ref_columns = ref_columns  # 被引用的列
        self.is_nullable = is_nullable


def test1_build_tables_structure():
    a = TableStructure("A")
    #a.add_column("x", False)
    a.add_column("id", False)
    a.set_primary_key("id")
    b = TableStructure("B")
    b.add_column("a_id", False)
    b.set_primary_key("a_id")
    b.add_foreign_key("a_id", "A", "id", False)
    c = TableStructure("C")
    c.add_column("id", False)
    c.set_primary_key("id")
    #c.add_foreign_key("a_id", "A", "id", False)
    d = TableStructure("D")
    d.add_column("c_id", False)
    d.set_primary_key("c_id")
    d.add_foreign_key("c_id", "C", "id", False)
    e = TableStructure("E")
    e.add_column("b_id", False)
    e.set_primary_key("b_id")
    e.add_foreign_key("b_id", "B", "a_id", False)

    # 客户表
    customers = TableStructure("Customers")
    customers.add_column("id", False)
    customers.add_column("category", False)  # 客户类别
    customers.set_primary_key("id")

    # 订单表
    orders = TableStructure("Orders")
    orders.add_column("id", False)
    orders.add_column("customer_id", False)
    orders.add_column("amount", False)  # 订单金额
    orders.set_primary_key("id")
    orders.add_foreign_key("customer_id", "Customers", "id", False)
    tables_structure = {"A": a, "B": b, "C": c, "D": d, "E": e,"Orders": orders,"Customers": customers}
    return tables_structure

def test2_build_tables_structure():
    # 基础维度表
    a = TableStructure("A")
    a.add_column("id", False)
    a.add_column("category", False)  # 用于分类聚合
    a.add_column("region", False)  # 用于多维分组
    a.add_column("sub_category", False)
    a.set_primary_key("id")

    # 事实表B，包含销售量和价格
    b = TableStructure("B")
    b.add_column("a_id", False)
    b.add_column("quantity", False)  # 用于SUM聚合
    b.add_column("price", False)  # 用于计算金额
    b.set_primary_key("a_id")
    b.add_foreign_key("a_id", "A", "id", False)

    # 另一个维度表
    c = TableStructure("C")
    c.add_column("id", False)
    c.add_column("type", False)  # 用于分类
    c.add_column("department", False)  # 用于多维分组
    c.set_primary_key("id")

    # 事实表D，包含金额数据
    d = TableStructure("D")
    d.add_column("c_id", False)
    d.add_column("amount", False)  # 用于SUM/AVG聚合
    d.add_column("tax", False)  # 用于额外计算
    d.set_primary_key("c_id")
    d.add_foreign_key("c_id", "C", "id", False)

    # 关联表E
    e = TableStructure("E")
    e.add_column("b_id", False)
    e.add_column("value", False)  # 用于测试AVG
    e.set_primary_key("b_id")
    e.add_foreign_key("b_id", "B", "a_id", False)

    # 新增表F和G，用于更复杂的连接
    f = TableStructure("F")
    f.add_column("id", False)
    f.add_column("category", False)  # 与A.category等价
    f.add_column("sales", False)  # 用于聚合
    f.set_primary_key("id")

    g = TableStructure("G")
    g.add_column("f_id", False)
    g.add_column("a_id", False)
    g.add_column("profit", False)  # 用于聚合
    g.set_primary_key("f_id")
    g.add_foreign_key("a_id", "A", "id", False)
    g.add_foreign_key("f_id", "F", "id", False)

    tables_structure = {
        "A": a, "B": b, "C": c,
        "D": d, "E": e, "F": f, "G": g
    }
    return tables_structure

def tpc_build_(sql_ddl,name):
    try:
        table_structure=TableStructure(name)
        parsed = sqlglot.parse_one(sql_ddl)
        if not isinstance(parsed, exp.Create):
            raise ValueError("不是CREATE TABLE语句")
        for column_def in parsed.find_all(exp.ColumnDef):
            column_name = column_def.this.name
            is_nullable = True
            for constraint in column_def.args.get("constraints", []):
                if str(constraint)=="NOT NULL":
                    is_nullable = False
            table_structure.add_column(column_name, is_nullable)
        return table_structure
    except Exception as e:
        print(f"解析失败: {e}")
        return None,None

def tpc_build_tables_structure():
    tables_structure={}
    for i,sql in enumerate(sqls):
        name=tables_name[i]
        table_structure=tpc_build_(sql,name)
        table_structure.set_primary_key(primary_keys[i])
        tables_structure[name]=table_structure
    return tables_structure

def find_table(column_name):
    if column_name is None or column_name=="":
        return None
    tables=[]
    try:
        tables_structure=tpc_build_tables_structure()
        for t_ in tables_structure:
            #print(t_,tables_structure[t_].columns)
            if tables_structure[t_].columns.get(column_name) is not None:
                tables.append(tables_structure[t_].name)
        if len(tables)!=1:
            print(f"{column_name} has {len(tables)} tables")
            #print(tables_structure)
            return None
        return tables[0]
    except Exception as e:
        return None


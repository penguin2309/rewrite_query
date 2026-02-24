# SQL查询优化系统（基于物化视图）

## 项目简介

这是一个基于物化视图的SQL查询优化系统，旨在通过匹配视图和查询（特别是TPCDS测试样例）来加速查询执行。系统能够处理复杂的SQL语句，包括子查询、CTE（公共表表达式）、UNION操作等，并通过智能视图匹配算法和查询重写技术提高查询性能。

## 核心功能

- **物化视图匹配**：自动匹配查询与现有物化视图，实现查询加速
- **复杂SQL支持**：处理子查询（EXISTS、IN）、CTE、UNION等复杂SQL结构
- **SPJG表达式处理**：支持Select-Project-Join-Group表达式的分析和转换
- **查询重写**：将原始查询重写为使用物化视图的等效查询
- **性能优化**：视图预解析、结构签名剪枝、表结构缓存
- **TPCDS测试支持**：专门针对TPCDS测试查询样例进行优化

## 项目结构

```
PythonProject1/
├── src/                # 源代码目录
│   ├── ViewMatcher.py          # 视图匹配主函数
│   ├── SPJGExpression.py       # SPJG表达式处理
│   ├── matcher_with_sub_q.py   # 带子查询的视图匹配
│   ├── spjg_exp_checker.py     # SQL语法验证
│   ├── TableStructure.py       # 表结构定义和缓存
│   ├── tpc_query_main.py       # TPCDS查询测试脚本
│   ├── mv_transfer.py          # 从SQL文件提取物化视图
│   ├── test_main.py            # 测试主文件
|   ├── config.py               # 配置文件（路径管理）
│   └── ...                     # 其他模块见代码
├── test/               # TPCDS测试查询样例
│   ├── query_1.sql
│   ├── query_2.sql
│   └── ...
├── m1_ddl.sql          # 物化视图定义
├── Optimizing Queries Using Materialized Views.pdf  # 参考论文
└── README.md           # 项目说明文档
```

## 核心模块

### 1. 视图匹配模块
- **ViewMatcher.py**：提供主视图匹配函数`view_match`
- **matcher_with_sub_q.py**：增强的视图匹配，支持子查询
- **spj_view_matcher.py**：SPJ（Select-Project-Join）视图匹配
- **agg_matcher.py**：聚合匹配与（必要时）rollup补偿

### 2. 表达式处理模块
- **SPJGExpression.py**：将SQL抽象为SPJG结构（表/列/谓词/group by/聚合）
- **spjg_exp_checker.py**：SQL语法验证和表达式检查

### 3. 辅助模块
- **TableStructure.py**：表结构定义和缓存，提高性能
- **EquivalenceClassManager.py**：等价类管理
- **PredicateClassifier.py**：谓词分类
- **join_eliminator.py**：连接消除
- **expr_checker.py**：表达式等价判断（优先基于sqlglot AST）

### 4. 测试模块
- **tpc_query_main.py**：TPCDS查询测试脚
- **test_main.py**：简单测试示例

## 匹配与重写流程（概览）

1. **加载视图定义**：`mv_transfer(m1_ddl.sql)` 解析 `CREATE TABLE ... AS SELECT ...` 并返回字典 `{view_name: view_sql}`。
2. **入口匹配**：`view_match(query_sql, views)`。
3. **外层结构处理（matcher_with_sub_q）**：
   - 递归处理 `WITH/子查询/UNION`；
   - 对 `WITH` 做依赖顺序处理，并维护“CTE→视图”的逻辑替换表，使下游看到的是实际视图/基表集合；
   - 统一做一定程度的AST规范化（如列限定、条件重排）并通过结构签名剪枝减少无效尝试。
4. **SPJG 匹配（spj_view_matcher + agg_matcher）**：
   - `spj_view_matcher` 负责 SPJ/谓词/输出可由视图计算等检查；
   - `agg_matcher` 负责聚合匹配与必要的 rollup/补偿重写。

## 技术特点

1. **强大的SQL解析能力**：使用sqlglot库解析和处理SQL语句
2. **复杂SQL支持**：处理子查询、CTE、UNION等复杂SQL结构
3. **智能视图匹配**：基于SPJG表达式的视图匹配算法
4. **查询重写优化**：自动将原始查询重写为使用物化视图的等效查询
5. **性能优化**：表结构缓存，减少重复解析时间
6. **TPCDS测试支持**：专门针对TPCDS测试查询样例进行优化

## 安装与使用

### 安装依赖

```bash
pip install sqlglot
```

### 配置管理

项目使用 `config.py` 文件统一管理所有路径配置：

```python
# config.py
DATA_PATH = "C:\\tpcds-data"
PROJECT_ROOT = "C:\\Users\\o2309\\PycharmProjects\\PythonProject1"
VIEW_DDL_FILE = f"{PROJECT_ROOT}\\m1_ddl.sql"
DATA_FILES = {
    "call_center": f"{DATA_PATH}\\call_center.dat",
    # ... 其他数据文件
}
```

### 基本使用

1. **定义物化视图**：在`m1_ddl.sql`文件中定义物化视图

2. **配置路径**：修改`config.py`中的路径配置以适应您的环境

3. **执行单个查询测试**：

```bash
python src/test_main.py
```

4. **执行TPCDS查询测试**：

```bash
python src/test_tpcds.py
```

### 示例代码

```python
from ViewMatcher import view_match
from mv_transfer import mv_transfer
from config import VIEW_DDL_FILE

# 提取物化视图（使用配置文件路径）
views = mv_transfer(VIEW_DDL_FILE)

# 原始查询
query_sql = """
select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales
 from wscs
     ,date_dim
 where d_date_sk =ws_sold_date_sk
 group by d_week_seq;
"""

# 执行视图匹配和查询重写
new_query_sql = view_match(query_sql, views)
print("重写后的SQL:")
print(new_query_sql)
```

## Quick Test：Query2 对齐视图

项目中已在 `m1_ddl.sql` 追加了 3 个用于测试 Query2 结构的视图：

- `mv_096`：对应 Query2 的 `wscs`（web_sales 与 catalog_sales 的 `UNION ALL`）
- `mv_097`：对应 Query2 的 `wswscs`（`mv_096` 与 `date_dim` join 后按 `d_week_seq` 聚合）
- `mv_098`：在 `mv_097` 基础上补 `d_year`，便于对齐子查询 `y/z` 中的年份谓词

## 测试结果

系统正在针对TPCDS测试查询样例进行了测试

## 已知限制

- 当前主要面向 SPJG / 常见TPC-DS形态，窗口函数等复杂特性未覆盖。
- 表结构与主外键关系依赖 `tpc_*` 文件提供的 schema 信息。

## 参考资料

- **论文**：Optimizing Queries Using Materialized Views
- **TPCDS测试套件**：用于测试SQL查询性能的标准测试套件

## 项目扩展

系统设计考虑了可扩展性，可以通过以下方式进行扩展：

1. **支持更多SQL特性**：添加对窗口函数、复杂表达式等的支持
2. **优化匹配算法**：进一步提高视图匹配的准确性和效率
3. **集成到实际系统**：与数据库系统集成，实现自动查询优化

## 贡献

欢迎提交问题和改进建议，共同完善这个SQL查询优化系统。

## 许可证

本项目采用MIT许可证。

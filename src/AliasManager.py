from sqlglot import parse_one, expressions as exp
from typing import Dict, List, Optional, Tuple, Set

# 避免循环导入，实现一个简单的find_table函数
def find_table(column_name: str) -> Optional[str]:
    """
    简单的列到表的映射函数
    注意：这是一个简化版本，实际应用中可能需要更复杂的逻辑
    """
    # 这里可以添加简单的列到表的映射逻辑
    # 例如：基于常见的命名约定或预定义的映射
    return None

class AliasManager:
    """
    别名管理器
    负责处理SQL查询中的别名关系，包括表别名和列别名
    支持复杂SQL结构，如CTE、子查询、视图等
    """
    
    def __init__(self):
        # 表别名映射: {alias: table_name}
        self.table_aliases: Dict[str, str] = {}
        # 列别名映射: {alias: (table_name, column_name)}
        self.column_aliases: Dict[str, Tuple[str, str]] = {}
        # 反向映射: {(table_name, column_name): alias}
        self.column_to_alias: Dict[Tuple[str, str], str] = {}
        # 视图别名映射: {view_name: {alias: (original_table, original_column)}}
        self.view_aliases: Dict[str, Dict[str, Tuple[str, str]]] = {}
        # CTE别名映射: {cte_name: AliasManager}
        self.cte_managers: Dict[str, 'AliasManager'] = {}
        # CTE定义映射: {cte_name: cte_sql}
        self.cte_definitions: Dict[str, str] = {}
    
    def add_table_alias(self, alias: str, table_name: str) -> None:
        """
        添加表别名
        """
        self.table_aliases[alias] = table_name
    
    def add_column_alias(self, alias: str, table_name: str, column_name: str) -> None:
        """
        添加列别名
        """
        self.column_aliases[alias] = (table_name, column_name)
        self.column_to_alias[(table_name, column_name)] = alias
    
    def add_view_alias(self, view_name: str, alias_mapping: Dict[str, Tuple[str, str]]) -> None:
        """
        添加视图别名映射
        """
        self.view_aliases[view_name] = alias_mapping
    
    def add_cte_manager(self, cte_name: str, manager: 'AliasManager') -> None:
        """
        添加CTE的别名管理器
        """
        self.cte_managers[cte_name] = manager
    
    def add_cte_definition(self, cte_name: str, cte_sql: str) -> None:
        """
        添加CTE定义
        """
        self.cte_definitions[cte_name] = cte_sql
    
    def get_table_by_alias(self, alias: str) -> Optional[str]:
        """
        根据别名获取表名
        """
        return self.table_aliases.get(alias)
    
    def get_column_by_alias(self, alias: str) -> Optional[Tuple[str, str]]:
        """
        根据别名获取列的(表名, 列名)
        """
        return self.column_aliases.get(alias)
    
    def get_alias_by_column(self, table_name: str, column_name: str) -> Optional[str]:
        """
        根据(表名, 列名)获取别名
        """
        return self.column_to_alias.get((table_name, column_name))
    
    def get_view_alias_mapping(self, view_name: str) -> Optional[Dict[str, Tuple[str, str]]]:
        """
        获取视图的别名映射
        """
        return self.view_aliases.get(view_name)
    
    def get_cte_manager(self, cte_name: str) -> Optional['AliasManager']:
        """
        获取CTE的别名管理器
        """
        return self.cte_managers.get(cte_name)
    
    def get_cte_definition(self, cte_name: str) -> Optional[str]:
        """
        获取CTE定义
        """
        return self.cte_definitions.get(cte_name)
    
    def resolve_column(self, column_ref: exp.Column, context: Optional[str] = None) -> Tuple[str, str]:
        """
        解析列引用，返回(表名, 列名)
        
        Args:
            column_ref: 列引用表达式
            context: 上下文，如CTE名称或视图名称
            
        Returns:
            (表名, 列名)
        """
        # 处理带表别名的列
        if column_ref.table:
            table_alias = str(column_ref.table)
            table_name = self.get_table_by_alias(table_alias)
            
            # 检查是否是CTE
            if table_alias in self.cte_managers:
                cte_manager = self.cte_managers[table_alias]
                return cte_manager.resolve_column(column_ref)
            
            # 检查是否是表别名
            if table_name:
                return table_name, str(column_ref.name)
            
            # 可能是原始表名
            return table_alias, str(column_ref.name)
        
        # 处理不带表别名的列
        column_name = str(column_ref.name)
        
        # 检查是否是列别名
        column_info = self.get_column_by_alias(column_name)
        if column_info:
            return column_info
        
        # 检查是否是CTE中的列
        if context and context in self.cte_managers:
            cte_manager = self.cte_managers[context]
            column_info = cte_manager.get_column_by_alias(column_name)
            if column_info:
                return column_info
        
        # 尝试通过表结构查找
        table_name = find_table(column_name)
        if table_name:
            return table_name, column_name
        
        # 对于复杂表达式中的列，尝试从上下文推断
        if context:
            # 这里可以添加更复杂的上下文推断逻辑
            pass
        
        # 最后尝试返回列名本身
        return "", column_name
    
    def build_from_query(self, query_sql: str) -> None:
        """
        从查询SQL构建别名映射
        """
        try:
            query_ast = parse_one(query_sql)
            self._process_query_ast(query_ast)
        except Exception as e:
            print(f"Error building alias manager from query: {e}")
    
    def _process_query_ast(self, query_ast: exp.Expression) -> None:
        """
        处理查询AST，构建别名映射
        """
        # 处理CTE
        with_clause = query_ast.args.get("with")
        if with_clause:
            for cte in with_clause.expressions:
                cte_name = cte.alias
                cte_sql = str(cte.this)
                self.add_cte_definition(cte_name, cte_sql)
                
                cte_manager = AliasManager()
                cte_manager._process_query_ast(cte.this)
                self.add_cte_manager(cte_name, cte_manager)
        
        # 处理FROM子句中的表别名
        from_clause = query_ast.args.get("from")
        if from_clause:
            self._process_from_clause(from_clause)
        
        # 处理JOIN子句中的表别名
        joins = query_ast.args.get("joins", [])
        for join in joins:
            self._process_from_clause(join.args.get("this"))
        
        # 处理SELECT子句中的列别名
        for expr in query_ast.expressions:
            self._process_select_expression(expr)
    
    def _process_from_clause(self, from_expr: exp.Expression) -> None:
        """
        处理FROM子句
        """
        if isinstance(from_expr, exp.Alias):
            # 处理表别名
            if isinstance(from_expr.this, exp.Table):
                table_name = from_expr.this.name
                alias = from_expr.alias
                self.add_table_alias(alias, table_name)
            # 处理子查询别名
            elif isinstance(from_expr.this, exp.Subquery):
                subquery_manager = AliasManager()
                subquery_manager._process_query_ast(from_expr.this.this)
                self.add_cte_manager(from_expr.alias, subquery_manager)
            # 处理CTE引用
            elif isinstance(from_expr.this, exp.Table):
                table_name = from_expr.this.name
                if table_name in self.cte_managers:
                    # 这是一个CTE引用
                    self.add_table_alias(from_expr.alias, table_name)
        elif isinstance(from_expr, exp.Table):
            # 处理无别名的表
            table_name = from_expr.name
            # 可以选择将表名也添加为别名
            self.add_table_alias(table_name, table_name)
        elif isinstance(from_expr, exp.Subquery):
            # 处理无别名的子查询
            subquery_manager = AliasManager()
            subquery_manager._process_query_ast(from_expr.this)
    
    def _process_select_expression(self, expr: exp.Expression) -> None:
        """
        处理SELECT表达式中的别名
        """
        if isinstance(expr, exp.Alias):
            alias = expr.alias
            # 处理列别名
            if isinstance(expr.this, exp.Column):
                try:
                    table_name, column_name = self.resolve_column(expr.this)
                    self.add_column_alias(alias, table_name, column_name)
                except ValueError:
                    # 无法解析列，可能是表达式
                    pass
            # 处理表达式别名
            elif self._is_complex_expression(expr.this):
                # 对于复杂表达式，尝试提取其中的列
                self._process_complex_expression(expr.this, alias)
            # 处理子查询别名
            elif isinstance(expr.this, exp.Subquery):
                # 子查询别名处理
                pass
        elif isinstance(expr, exp.Column):
            # 处理无别名的列
            try:
                table_name, column_name = self.resolve_column(expr)
                # 可以选择将列名也添加为别名
                self.add_column_alias(column_name, table_name, column_name)
            except ValueError:
                pass
    
    def _process_complex_expression(self, expr: exp.Expression, alias: str) -> None:
        """
        处理复杂表达式，提取其中的列引用
        """
        # 递归处理复杂表达式
        if hasattr(expr, 'this') and expr.this:
            self._process_complex_expression(expr.this, alias)
        if hasattr(expr, 'expression') and expr.expression:
            self._process_complex_expression(expr.expression, alias)
        if hasattr(expr, 'expressions') and expr.expressions:
            for sub_expr in expr.expressions:
                self._process_complex_expression(sub_expr, alias)
        # 处理列引用
        if isinstance(expr, exp.Column):
            try:
                table_name, column_name = self.resolve_column(expr)
                # 为复杂表达式中的列添加映射
                self.add_column_alias(alias, table_name, column_name)
            except ValueError:
                pass
    
    def _is_complex_expression(self, expr: exp.Expression) -> bool:
        """
        判断是否是复杂表达式
        """
        complex_types = (
            exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Cast, exp.Round,
            exp.Sum, exp.Avg, exp.Count, exp.Max, exp.Min, exp.Substring,
            exp.Alias, exp.Subquery, exp.Table, exp.Column
        )
        return isinstance(expr, complex_types)
    
    def build_from_view(self, view_name: str, view_sql: str) -> None:
        """
        从视图SQL构建别名映射
        """
        try:
            view_ast = parse_one(view_sql)
            view_manager = AliasManager()
            view_manager._process_query_ast(view_ast)
            
            # 构建视图到原始表的映射
            alias_mapping = {}
            for alias, (table_name, column_name) in view_manager.column_aliases.items():
                alias_mapping[alias] = (table_name, column_name)
            
            self.add_view_alias(view_name, alias_mapping)
        except Exception as e:
            print(f"Error building alias manager from view: {e}")
    
    def get_view_resolved_columns(self, view_name: str) -> Dict[str, Tuple[str, str]]:
        """
        获取视图解析后的列映射
        """
        return self.view_aliases.get(view_name, {})
    
    def resolve_view_column(self, view_name: str, column_name: str) -> Optional[Tuple[str, str]]:
        """
        解析视图中的列
        """
        view_mapping = self.get_view_resolved_columns(view_name)
        return view_mapping.get(column_name)
    
    def create_rewrite_map(self, view_name: str) -> Dict[str, str]:
        """
        创建重写映射，用于将原始查询中的列映射到视图中的列
        """
        rewrite_map = {}
        view_mapping = self.get_view_resolved_columns(view_name)
        
        for alias, (table_name, column_name) in view_mapping.items():
            # 构建原始列引用
            if table_name:
                original_ref = f"{table_name}.{column_name}"
            else:
                original_ref = column_name
            # 构建视图列引用
            view_ref = f"{view_name}.{alias}"
            rewrite_map[original_ref] = view_ref
        
        return rewrite_map


def build_alias_manager(query_sql: str, view_sqls: Optional[Dict[str, str]] = None) -> AliasManager:
    """
    构建别名管理器
    
    Args:
        query_sql: 查询SQL
        view_sqls: 视图SQL映射 {view_name: view_sql}
        
    Returns:
        AliasManager实例
    """
    manager = AliasManager()
    manager.build_from_query(query_sql)
    
    # 处理视图
    if view_sqls:
        for view_name, view_sql in view_sqls.items():
            manager.build_from_view(view_name, view_sql)
    
    return manager


def resolve_aliases_in_query(query_sql: str, view_sqls: Optional[Dict[str, str]] = None) -> Tuple[AliasManager, str]:
    """
    解析查询中的别名，并返回解析后的SQL
    
    Args:
        query_sql: 查询SQL
        view_sqls: 视图SQL映射
        
    Returns:
        (AliasManager, 解析后的SQL)
    """
    manager = build_alias_manager(query_sql, view_sqls)
    
    # 这里可以添加SQL重写逻辑
    # 目前返回原始SQL
    return manager, query_sql


def get_alias_mapping(query_sql: str) -> Dict[str, Dict[str, str]]:
    """
    获取查询中的别名映射
    
    Returns:
        {"table_aliases": {alias: table_name}, "column_aliases": {alias: column_name}}
    """
    manager = AliasManager()
    manager.build_from_query(query_sql)
    
    result = {
        "table_aliases": manager.table_aliases,
        "column_aliases": {}
    }
    
    for alias, (table_name, column_name) in manager.column_aliases.items():
        if table_name:
            result["column_aliases"][alias] = f"{table_name}.{column_name}"
        else:
            result["column_aliases"][alias] = column_name
    
    return result
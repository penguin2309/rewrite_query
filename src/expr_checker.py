import sympy as sp
from sympy import symbols, sympify, simplify
from collections import defaultdict
from typing import List, Tuple, Union
from sqlglot import expressions

def is_exp_eq(exp1: str, exp2: str, eq_classes= None) ->bool:
    """
    判断两个表达式是否在给定等价类条件下等价

    Parameters:
    -----------
    exp1, exp2 : str
        要比较的表达式字符串
    eq_classes : List[List[str]], optional
        等价类列表，如 [['a','b','c'], ['d','e']]
        表示a,b,c等价，d,e等价

    Returns:
    --------
    Tuple[bool, str, str]
        (是否等价, 简化后的表达式1, 简化后的表达式2)
    """
    #print(exp1, exp2, eq_classes)
    try:
        if isinstance(exp1, expressions.Cast) and isinstance(exp2, expressions.Cast):
            return exp1.this.this==exp2.this.this and exp1.to==exp2.to
        if isinstance(exp1, expressions.Round) and isinstance(exp2, expressions.Round):
            return exp1.this==exp2.this and exp1.args["decimals"]==exp2.args["decimals"]

        # 解析表达式
        expr1 = sympify(exp1)
        expr2 = sympify(exp2)

        # 提取表达式中的所有符号
        symbols1 = expr1.free_symbols
        symbols2 = expr2.free_symbols
        all_symbols = symbols1.union(symbols2)

        # 构建等价代换字典
        substitution_dict = {}
        if eq_classes:
            # 为每个等价类创建一个映射关系
            eq_class_map = {}
            for eq_class in eq_classes:
                if eq_class:  # 确保非空
                    # 获取sympy符号对象
                    #sympy_symbols = [sp.Symbol(var) for var.col in eq_class]
                    sympy_symbols=[]
                    for c in eq_class:
                        sympy_symbols.append(sp.Symbol(c.col))
                    # 选择第一个符号作为标准
                    standard_symbol = sympy_symbols[0]

                    # 其他符号映射到标准符号
                    for symbol in sympy_symbols[1:]:
                        substitution_dict[symbol] = standard_symbol

            # 验证所有符号都在等价类中定义（可选）
            # 如果不在等价类中的符号，保持原样

        # 应用等价代换
        if substitution_dict:
            expr1_sub = expr1.subs(substitution_dict)
            expr2_sub = expr2.subs(substitution_dict)
        else:
            expr1_sub = expr1
            expr2_sub = expr2

        # 简化表达式
        expr1_simplified = simplify(expr1_sub)
        expr2_simplified = simplify(expr2_sub)

        # 判断是否等价
        diff = simplify(expr1_simplified - expr2_simplified)
        is_equivalent = (diff == 0)
        return is_equivalent

    except Exception as e:
        #print('\033[93m',exp1)
        print('\033[92mIN expr_eq_checker:',e,'使用字符串匹配\033[0m')
        return exp1==exp2
        #raise ValueError(f"表达式解析或处理失败: {e}")


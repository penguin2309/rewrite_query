import re
from typing import Any, Dict, Iterable, Optional, Tuple

import sympy as sp
from sympy import sympify, simplify
from sqlglot import expressions as exp
from sqlglot import parse_one

_QUALIFIER_RE = re.compile(r"\b[A-Za-z_]\w*\.")
_QUOTE_RE = re.compile(r"[`\"]")


def _expr_from_any(v: Any) -> Optional[exp.Expression]:
    if isinstance(v, exp.Expression):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return parse_one(s)
        except Exception:
            return None
    return None


def _col_key(obj: Any) -> Optional[Tuple[Optional[str], str]]:
    table = getattr(obj, "table", None)
    col = getattr(obj, "col", None)
    if not col:
        return None
    table = table or None
    return table, str(col)


def _eq_index_map(eq_classes: Any) -> Dict[Tuple[Optional[str], str], int]:
    out: Dict[Tuple[Optional[str], str], int] = {}
    if not eq_classes:
        return out
    for i, cls in enumerate(eq_classes):
        if not cls:
            continue
        for item in cls:
            key = _col_key(item)
            if not key:
                continue
            out[key] = i
            out[(None, key[1])] = i
    return out


def _flatten_binop(node: exp.Expression, cls: type) -> list[exp.Expression]:
    if isinstance(node, cls):
        return _flatten_binop(node.this, cls) + _flatten_binop(node.expression, cls)
    return [node]


def _build_binop(parts: list[exp.Expression], cls: type) -> exp.Expression:
    out = parts[0]
    for p in parts[1:]:
        out = cls(this=out, expression=p)
    return out


def _canonicalize_expr(node: exp.Expression, eq_map: Dict[Tuple[Optional[str], str], int]) -> exp.Expression:
    def transform(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column):
            key = (n.table or None, n.name)
            idx = eq_map.get(key)
            if idx is None:
                idx = eq_map.get((None, n.name))
            if idx is not None:
                return exp.Column(this=exp.Identifier(this=f"__eq{idx}__"))
            return n
        if isinstance(n, exp.EQ):
            l = n.this
            r = n.expression
            l_s = l.sql(normalize=True)
            r_s = r.sql(normalize=True)
            if r_s < l_s:
                return exp.EQ(this=r, expression=l)
            return n
        if isinstance(n, exp.Add):
            parts = _flatten_binop(n, exp.Add)
            parts = [p.transform(transform) for p in parts]
            parts.sort(key=lambda x: x.sql(normalize=True))
            return _build_binop(parts, exp.Add)
        if isinstance(n, exp.Mul):
            parts = _flatten_binop(n, exp.Mul)
            parts = [p.transform(transform) for p in parts]
            parts.sort(key=lambda x: x.sql(normalize=True))
            return _build_binop(parts, exp.Mul)
        return n

    return node.transform(transform)


def _is_sympy_safe(node: exp.Expression) -> bool:
    for n in node.walk():
        if isinstance(
            n,
            (
                exp.Case,
                exp.Cast,
                exp.Anonymous,
                exp.Func,
                exp.RegexpLike,
                exp.In,
                exp.Between,
                exp.Like,
            ),
        ):
            return False
    return True


def _canon_for_sympy(s: str) -> str:
    s = _QUOTE_RE.sub("", s)
    s = _QUALIFIER_RE.sub("", s)
    s = re.sub(r"\s+", " ", s.strip())
    return s


def is_exp_eq(exp1: Any, exp2: Any, eq_classes=None) -> bool:
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
    try:
        e1 = _expr_from_any(exp1)
        e2 = _expr_from_any(exp2)
        if e1 is not None and e2 is not None:
            if isinstance(e1, exp.Cast) and isinstance(e2, exp.Cast):
                return e1.this.this == e2.this.this and e1.to == e2.to
            if isinstance(e1, exp.Round) and isinstance(e2, exp.Round):
                return e1.this == e2.this and e1.args.get("decimals") == e2.args.get("decimals")

            eq_map = _eq_index_map(eq_classes)
            c1 = _canonicalize_expr(e1, eq_map)
            c2 = _canonicalize_expr(e2, eq_map)
            s1 = c1.sql(normalize=True)
            s2 = c2.sql(normalize=True)
            if s1 == s2:
                return True

            if _is_sympy_safe(c1) and _is_sympy_safe(c2):
                try:
                    x1 = sympify(_canon_for_sympy(s1))
                    x2 = sympify(_canon_for_sympy(s2))
                    return simplify(x1 - x2) == 0
                except Exception:
                    return False

            return False

        s1 = str(exp1).strip()
        s2 = str(exp2).strip()
        if not s1 or not s2:
            return s1 == s2
        try:
            p1 = parse_one(s1)
            p2 = parse_one(s2)
            eq_map = _eq_index_map(eq_classes)
            c1 = _canonicalize_expr(p1, eq_map).sql(normalize=True)
            c2 = _canonicalize_expr(p2, eq_map).sql(normalize=True)
            return c1 == c2
        except Exception:
            n1 = re.sub(r"\s+", " ", _canon_for_sympy(s1).upper())
            n2 = re.sub(r"\s+", " ", _canon_for_sympy(s2).upper())
            return n1 == n2

    except Exception as e:
        return str(exp1).strip() == str(exp2).strip()


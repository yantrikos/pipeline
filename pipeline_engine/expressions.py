"""Sandboxed expression evaluator for workflow templates.

Handles {{expression}} templates in node configs.
No eval(), no exec(), no filesystem access.
"""

import re
import json
import logging
from typing import Any, Optional

logger = logging.getLogger("pipeline.expressions")

TEMPLATE_PATTERN = re.compile(r'\{\{(.+?)\}\}')


def resolve_template(template: str, context: dict) -> str:
    """Resolve {{expression}} templates in a string."""
    if not isinstance(template, str):
        return template

    def replacer(match):
        expr = match.group(1).strip()
        value = resolve_path(expr, context)
        if value is None:
            return match.group(0)  # Leave unresolved
        return str(value)

    return TEMPLATE_PATTERN.sub(replacer, template)


def resolve_path(path: str, context: dict) -> Any:
    """Resolve a dot-path expression against a context dict.

    Supports: nodes.step1.output.data[0].name, inputs.user_id
    """
    current = context
    parts = _tokenize_path(path)

    for part in parts:
        if current is None:
            return None
        if isinstance(part, int):
            if isinstance(current, (list, tuple)) and 0 <= part < len(current):
                current = current[part]
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None

    return current


def resolve_dict(obj: Any, context: dict) -> Any:
    """Recursively resolve templates in a dict/list/string."""
    if isinstance(obj, str):
        return resolve_template(obj, context)
    elif isinstance(obj, dict):
        return {k: resolve_dict(v, context) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_dict(item, context) for item in obj]
    return obj


def evaluate_condition(expression: str, context: dict) -> bool:
    """Evaluate a simple boolean expression.

    Supports: field == value, field != value, field > value, field < value
    """
    # Parse simple comparison
    for op_str, op_fn in [
        ("==", lambda a, b: a == b),
        ("!=", lambda a, b: a != b),
        (">=", lambda a, b: a >= b if a is not None else False),
        ("<=", lambda a, b: a <= b if a is not None else False),
        (">", lambda a, b: a > b if a is not None else False),
        ("<", lambda a, b: a < b if a is not None else False),
    ]:
        if op_str in expression:
            parts = expression.split(op_str, 1)
            if len(parts) == 2:
                left = resolve_path(parts[0].strip(), context)
                right_str = parts[1].strip().strip("'\"")
                # Try to parse right side
                try:
                    right = json.loads(right_str)
                except (json.JSONDecodeError, ValueError):
                    right = right_str
                return op_fn(left, right)

    # If no operator, resolve as truthy check
    value = resolve_path(expression.strip(), context)
    return bool(value)


def _tokenize_path(path: str) -> list:
    """Tokenize a dot-path with array access: a.b[0].c -> ['a', 'b', 0, 'c']"""
    tokens = []
    for part in path.split("."):
        # Check for array index: name[0]
        bracket = part.find("[")
        if bracket >= 0:
            name = part[:bracket]
            if name:
                tokens.append(name)
            idx_str = part[bracket + 1:part.find("]")]
            try:
                tokens.append(int(idx_str))
            except ValueError:
                tokens.append(idx_str)
        else:
            tokens.append(part)
    return tokens

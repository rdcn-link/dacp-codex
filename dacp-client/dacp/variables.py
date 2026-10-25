"""Typed workflow value substitution, before submitting an immutable job snapshot."""
import json
import re
import yaml

_VARIABLE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)\Z")

def resolve_variables(text, variables=None):
    if variables is None:
        return text
    if not isinstance(variables, dict):
        raise ValueError("variables must be a JSON object")
    for key in variables:
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError("invalid workflow variable name")
    # Validate and copy JSON values; inserted strings are never expanded again.
    values = json.loads(json.dumps(variables, allow_nan=False))
    document = yaml.safe_load(text)
    def visit(value, depth=0):
        if depth > 100:
            raise ValueError("workflow nesting exceeds 100 levels")
        if isinstance(value, str):
            match = _VARIABLE.fullmatch(value)
            if match:
                name = match.group(1)
                if name not in values:
                    raise ValueError(f"missing workflow variable: {name}")
                return values[name]
        elif isinstance(value, list):
            return [visit(v, depth+1) for v in value]
        elif isinstance(value, dict):
            return {k: visit(v, depth+1) for k, v in value.items()}
        return value
    return json.dumps(visit(document), allow_nan=False)

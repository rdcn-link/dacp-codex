"""Typed workflow value substitution, before submitting an immutable job snapshot."""
import json
import re
from . import yaml_document

_VARIABLE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)\Z")

def resolve_variables(text, variables=None):
    if variables is None:
        return text
    if not isinstance(variables, dict):
        raise ValueError("variables must be a mapping")
    for key in variables:
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError("invalid workflow variable name")
    # Validate and copy JSON values; inserted strings are never expanded again.
    values = json.loads(json.dumps(variables, allow_nan=False))
    document = yaml_document.load(text)
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
    return yaml_document.dump(visit(document))


def remote_job(text):
    """Reject local authoring definitions before submitting or inferring remotely."""
    document = yaml_document.load(text)
    message = "Jobs containing inline definitions or local/source operators cannot be submitted to a remote DACP or Union server; publish the operators first"
    if not isinstance(document, dict):
        raise ValueError("job must be a mapping")
    if "operators" in document:
        raise ValueError(message)
    for step in document.get("steps", []):
        operator = step.get("operator") if isinstance(step, dict) else None
        if not isinstance(operator, str) or not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*", operator):
            raise ValueError(message)
    return text

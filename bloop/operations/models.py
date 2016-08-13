from ..exceptions import ConstraintViolation

__all__ = ["handle_constraint_violation", "standardize_query_response"]


def handle_constraint_violation(error, operation, item):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation(operation, item)
    else:
        raise error


def standardize_query_response(response):
    count = response.setdefault("Count", 0)
    response["ScannedCount"] = response.get("ScannedCount", count)

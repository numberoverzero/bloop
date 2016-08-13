from ..exceptions import ConstraintViolation

__all__ = ["handle_constraint_violation"]


def handle_constraint_violation(error, operation, item):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation(operation, item)
    else:
        raise error

"""Data validation helpers."""
from typing import Any


def validate_field(value: Any, field_name: str, allow_null: bool = False) -> Any:
    if value is None and not allow_null:
        raise ValueError(f"Field '{field_name}' cannot be null")
    return value


def validate_range(value: float, min_val: float | None = None, max_val: float | None = None, field_name: str = "value") -> float:
    if min_val is not None and value < min_val:
        raise ValueError(f"Field '{field_name}' value {value} below minimum {min_val}")
    if max_val is not None and value > max_val:
        raise ValueError(f"Field '{field_name}' value {value} above maximum {max_val}")
    return value


def validate_non_negative(value: float, field_name: str = "value") -> float:
    return validate_range(value, min_val=0.0, field_name=field_name)

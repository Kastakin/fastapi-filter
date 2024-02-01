# -*- coding: utf-8 -*-
from enum import Enum
import re
from typing import Union
from warnings import warn

from pydantic import ValidationInfo, field_validator
from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import Query, class_mapper, RelationshipProperty
from sqlalchemy.sql.selectable import Select

from ...base.filter import BaseFilterModel


def _backward_compatible_value_for_like_and_ilike(value: str):
    """Add % if not in value to be backward compatible.

    Args:
        value (str): The value to filter.

    Returns:
        Either the unmodified value if a percent sign is present, the value wrapped in % otherwise to preserve
        current behavior.
    """
    if "%" not in value:
        warn(
            "You must pass the % character explicitly to use the like and ilike operators.",
            DeprecationWarning,
            stacklevel=2,
        )
        value = f"%{value}%"
    return value


_orm_operator_transformer = {
    "neq": lambda value: ("__ne__", value, None),
    "gt": lambda value: ("__gt__", value, None),
    "gte": lambda value: ("__ge__", value, None),
    "in": lambda value: ("in_", value, None),
    "isnull": lambda value: ("is_", None, None) if value is True else ("is_not", None, None),
    "lt": lambda value: ("__lt__", value, None),
    "lte": lambda value: ("__le__", value, None),
    "between": lambda value: ("between", (value[0], value[1]), None),
    "like": lambda value: ("like", _backward_compatible_value_for_like_and_ilike(value), None),
    "ilike": lambda value: ("ilike", _backward_compatible_value_for_like_and_ilike(value), None),
    # XXX(arthurio): Mysql excludes None values when using `in` or `not in` filters.
    "not": lambda value: ("is_not", value, None),
    "not_in": lambda value: ("not_in", value, None),
    "and__between": lambda value: ("between", [(v[0], v[1]) for v in value], "and_"),
    "or__between": lambda value: ("between", [(v[0], v[1]) for v in value], "or_"),
}
"""Operators à la Django.

Examples:
    my_datetime__gte
    count__lt
    name__isnull
    user_id__in
"""


class Filter(BaseFilterModel):
    """Base filter for orm related filters.

    All children must set:
        ```python
        class Constants(Filter.Constants):
            model = MyModel
        ```

    It can handle regular field names and Django style operators.

    Example:
        ```python
        class MyModel:
            id: PrimaryKey()
            name: StringField(nullable=True)
            count: IntegerField()
            created_at: DatetimeField()

        class MyModelFilter(Filter):
            id: Optional[int]
            id__in: Optional[str]
            count: Optional[int]
            count__lte: Optional[int]
            created_at__gt: Optional[datetime]
            name__isnull: Optional[bool]
    """

    class Direction(str, Enum):
        asc = "asc"
        desc = "desc"

    @field_validator("*", mode="before")
    def split_str(cls, value, field: ValidationInfo):
        if field.field_name is not None and isinstance(value, str):
            if field.field_name.endswith("__or__between") or field.field_name.endswith("__and__between"):
                if not value:
                    # Empty string should return [] not ['']
                    return []
                # Create a pattern to match the string between square brackets
                # Example matches: [1,2],[3,4],[5,6]
                # or [[1,2],[3,4],[5,6]]
                pattern = re.compile(r"\[([^[\]]*)\]")

                # Find all matches of the pattern in the input string
                matches = pattern.findall(value)

                # Convert each match to a list of lists
                return [list(match.split(",")) for match in matches]

            elif (
                field.field_name == cls.Constants.ordering_field_name
                or field.field_name.endswith("__in")
                or field.field_name.endswith("__not_in")
                or field.field_name.endswith("__between")
            ):
                if not value:
                    # Empty string should return [] not ['']
                    return []
                return list(value.split(","))
        return value

    def filter(self, query: Union[Query, Select]):
        for field_name, value in self.filtering_fields:
            field_value = getattr(self, field_name)
            if isinstance(field_value, Filter):
                query = field_value.filter(query)
            else:
                if "__" in field_name:
                    field_name, operator = field_name.split("__", maxsplit=1)
                    operator, value, modifier = _orm_operator_transformer[operator](value)
                else:
                    operator = "__eq__"

                if field_name == self.Constants.search_field_name and hasattr(self.Constants, "search_model_fields"):
                    search_filters = [
                        getattr(self.Constants.model, field).ilike(f"%{value}%")
                        for field in self.Constants.search_model_fields
                    ]
                    query = query.filter(or_(*search_filters))
                else:
                    model_field = getattr(self.Constants.model, field_name)
                    if isinstance(value, tuple):
                        query = query.filter(getattr(model_field, operator)(*value))
                    elif isinstance(value, list) and all(isinstance(el, tuple) for el in value):
                        conditions = [getattr(model_field, operator)(*v) for v in value]

                        if modifier == "and_":
                            query = query.filter(and_(*conditions))
                        elif modifier == "or_":
                            query = query.filter(or_(*conditions))
                    else:
                        query = query.filter(getattr(model_field, operator)(value))
        return query

    def sort(self, query: Union[Query, Select]):
        if not self.ordering_values:
            return query

        for field_name in self.ordering_values:
            direction = Filter.Direction.asc
            if field_name.startswith("-"):
                direction = Filter.Direction.desc
            field_name = field_name.replace("-", "").replace("+", "")

            order_by_field = getattr(self.Constants.model, field_name)

            query = query.order_by(getattr(order_by_field, direction)())

        return query

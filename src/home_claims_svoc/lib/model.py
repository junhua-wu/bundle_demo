import re
from enum import Enum
from pydantic import Field

from typing import Optional, List
from decimal import Decimal
import datetime as dt
from datetime import datetime

from humps import camelize
from pydantic import ConfigDict
from sparkdantic import SparkModel
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    DecimalType,
)


class PascalSchema(SparkModel):
    model_config = ConfigDict(
        alias_generator=camelize, populate_by_name=True, use_enum_values=False
    )

    @classmethod
    def standardize_field_name(cls, name):
        """Standardize field names by inserting underscores between lowercase and uppercase letters, replacing spaces and hyphens with underscores, and converting to lowercase."""
        # Insert underscores between lowercase and uppercase letters
        name = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name)
        # Replace spaces and hyphens with underscores
        name = re.sub(r"[\s\-]+", "_", name)
# Convert the entire name to lowercase
        return name.lower()

    @classmethod
    def recursive_standardize_schema(cls, field, depth=0):
        """Recursively standardize field names and adjust schema for nested structures with depth tracking."""
        new_depth = depth + 1

        if isinstance(field.dataType, StructType):
            new_fields = [
                cls.recursive_standardize_schema(f, new_depth)
                for f in field.dataType.fields
            ]
            
            new_data_type = StructType(new_fields)
        elif isinstance(field.dataType, ArrayType):
            element_type = cls.recursive_standardize_schema(
                StructField("element", field.dataType.elementType, True), new_depth
            )
            new_data_type = ArrayType(
                element_type.dataType, True
            )  # Arrays inherently contain nullable elements
        elif isinstance(field.dataType, MapType):
            key_type = cls.recursive_standardize_schema(
                StructField("key", field.dataType.keyType, True), new_depth
            )
            value_type = cls.recursive_standardize_schema(
                StructField("value", field.dataType.valueType, True), new_depth
            )
            new_data_type = MapType(key_type.dataType, value_type.dataType, True)
        # typecast pydantic decimal to pyspark DecimalType with set precision and scale
        elif isinstance(field.dataType, DecimalType):
                    new_data_type = DecimalType(13, 1)
        else:
            new_data_type = field.dataType

        standardized_name = cls.standardize_field_name(field.name)

        new_nullable = True if depth >= 2 else field.nullable

        return StructField(standardized_name, new_data_type, new_nullable)

    @classmethod
    def model_spark_schema_standardized(cls) -> StructType:
        """Generates a PySpark schema from the model fields with standardized and adjusted nullability."""
        original_schema = cls.model_spark_schema()
        standardized_fields = [
            cls.recursive_standardize_schema(field, 0)
            for field in original_schema.fields
        ]

        return StructType(standardized_fields)


class TestTable(PascalSchema):
    test: str



test_list = [
    {
        "table_name": "test",
        "schema": TestTable.model_spark_schema_standardized(),
    }
]
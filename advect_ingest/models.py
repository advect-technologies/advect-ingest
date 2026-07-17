import datetime as dt
from typing import Any, Annotated

from pydantic import BaseModel, Field, AfterValidator
from daq_tools.models import DataPoint as DaqDataPoint

def validate_tags(tags: dict[str, str]) -> dict[str, str]:
    if 'id' not in tags:
        raise ValueError('id tag is required')
    return tags

def validate_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return fields

class DataPoint(BaseModel):
    measurement: str
    time: float | int = Field(
        default_factory=lambda: dt.datetime.now(dt.timezone.utc).timestamp(),
        description='Unix timestamp in seconds'
    )
    tags: Annotated[dict[str, str], AfterValidator(validate_tags)] = Field(
        default_factory=dict,
        description='Tags for the data point'
    )
    fields: Annotated[dict[str, float | int | str | bool | None], AfterValidator(validate_fields)] = Field(
        default_factory=dict,
        description='Fields for the data point'
    )

    def to_daq_point(self) -> DaqDataPoint:
        return DaqDataPoint(
            measurement=self.measurement,
            time=self.time,
            tags=self.tags,
            fields=self.fields
        )

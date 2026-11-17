import datetime as dt
from typing import Annotated, Any

from daq_tools.models import DataPoint as DaqDataPoint
from pydantic import AfterValidator, BaseModel, Field, model_validator


def validate_tags(tags: dict[str, str]) -> dict[str, str]:
    if "id" not in tags:
        raise ValueError("id tag is required")
    return tags


def validate_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return fields


class DataPoint(BaseModel):
    measurement: str
    time: float | int = Field(
        default_factory=lambda: dt.datetime.now(dt.UTC).timestamp(),
        description="Unix timestamp in seconds",
    )
    tags: Annotated[dict[str, str], AfterValidator(validate_tags)] = Field(
        default_factory=dict, description="Tags for the data point"
    )
    fields: Annotated[
        dict[str, float | int | str | bool | None], AfterValidator(validate_fields)
    ] = Field(default_factory=dict, description="Fields for the data point")

    def to_daq_point(self) -> DaqDataPoint:
        return DaqDataPoint(
            measurement=self.measurement,
            time=self.time,
            tags=self.tags,
            fields=self.fields,
        )


class LitePoint(BaseModel):
    """A single sample inside a litebatch payload."""

    fields: dict[str, float | int | str | bool | None]
    tags: dict[str, str] = Field(default_factory=dict)
    time: float | int | None = None  # absolute unix timestamp
    rtime_ms: int | None = None
    rtime_s: float | int | None = None

    @model_validator(mode="after")
    def require_time_or_rtime(self) -> "LitePoint":
        if self.time is None and self.rtime_ms is None and self.rtime_s is None:
            raise ValueError("Point must have either 'time' or 'rtime_ms'/'rtime_s'")
        return self


class LiteBatch(BaseModel):
    """Batch payload from low-power devices."""

    measurement: str
    global_tags: Annotated[dict[str, str], AfterValidator(validate_tags)]
    points: list[LitePoint]

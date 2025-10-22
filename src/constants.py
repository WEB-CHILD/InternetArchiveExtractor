from enum import Enum


class Period(Enum):
    """Enum for the different periods around the archived date to download."""
    DAY = "DAY"
    WEEK = "WEEK"


# Default download period
DOWNLOAD_PERIOD = Period.DAY

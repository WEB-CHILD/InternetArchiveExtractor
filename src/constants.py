from enum import Enum


class Period(Enum):
    """Enum for the different periods around the archived date to download."""
    DAY = "DAY"
    WEEK = "WEEK"
    FULL = "FULL"
    CUSTOM = "CUSTOM"


# Default download period
DOWNLOAD_PERIOD = Period.DAY

# Download reset 
DOWNLOAD_RESET = False

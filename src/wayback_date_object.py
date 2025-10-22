from datetime import datetime, timedelta


class WaybackDateObject:
    def __init__(self, waybackdate_str):
        self.year = waybackdate_str[0:4]
        self.month = waybackdate_str[4:6]
        self.day = waybackdate_str[6:8]
        self.hour = waybackdate_str[8:10]
        self.minute = waybackdate_str[10:12]
        self.second = waybackdate_str[12:14]

    @classmethod
    def from_values(self, year, month, day, hour, minute, second):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second

    def pretty_print(self):
        """
        Returns a human-readable string representation of the date and time.
        Example: '2003-04-09 19:30:11'
        """
        return f"{self.year}-{self.month}-{self.day} {self.hour}:{self.minute}:{self.second}"
    
    def wayback_format(self):
        """
        Returns the date and time in the original Wayback date format.
        Example: '20030409193011'
        """
        return f"{self.year}{self.month}{self.day}{self.hour}{self.minute}{self.second}"
    
    def increment_day(self):
        """Use datetime arithmetic to add one day so month lengths and
        leap years are handled correctly."""
        dt = self.to_datetime() + timedelta(days=1)
        self.from_datetime(dt)

    def decrement_day(self):
        """
        Decrements the date by one day, adjusting month and year as necessary.
        This method uses Python's datetime arithmetic so it correctly handles
        varying month lengths and leap years.
        """
        dt = self.to_datetime() - timedelta(days=1)
        self.from_datetime(dt)

    def to_datetime(self):
        """Converts the WaybackDateObject to a Python datetime object."""
        return datetime(
            int(self.year), int(self.month), int(self.day),
            int(self.hour), int(self.minute), int(self.second)
        )

    def from_datetime(self, dt):
        """Updates the WaybackDateObject from a Python datetime object."""
        self.year = f"{dt.year:04d}"
        self.month = f"{dt.month:02d}"
        self.day = f"{dt.day:02d}"
        self.hour = f"{dt.hour:02d}"
        self.minute = f"{dt.minute:02d}"
        self.second = f"{dt.second:02d}"

    def increment_week(self):
        """
        Increments the date by 7 days.
        """
        dt = self.to_datetime() + timedelta(days=7)
        self.from_datetime(dt)

    def decrement_week(self):
        """
        Decrements the date by 7 days.
        """
        dt = self.to_datetime() - timedelta(days=7)
        self.from_datetime(dt)


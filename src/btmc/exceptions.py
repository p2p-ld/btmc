class PeerError(Exception):
    """The peer broke the protocol"""


class TrackerError(Exception):
    """The tracker refused or sent something we can't use"""


class MissingData(Exception):
    """
    Raised when a read touches bytes we haven't fetched yet.

    Not an OSError, because zipfile turns those into a generic BadZipFile.
    """

    def __init__(self, start: int, end: int):
        super().__init__(f"missing bytes [{start}, {end})")
        self.start = start
        self.end = end

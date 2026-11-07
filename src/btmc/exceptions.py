class PeerError(Exception):
    """The peer broke the protocol"""


class TrackerError(Exception):
    """The tracker didn't answer, refused, or sent something we can't use"""


class TrackerRefused(TrackerError):
    """
    The tracker answered, but refused the announce.
    It's up, and might take announces for other torrents.
    """


class MissingData(Exception):
    """
    Raised when a read touches bytes we haven't fetched yet.

    Not an OSError, because zipfile turns those into a generic BadZipFile.
    """

    def __init__(self, start: int, end: int):
        super().__init__(f"missing bytes [{start}, {end})")
        self.start = start
        self.end = end

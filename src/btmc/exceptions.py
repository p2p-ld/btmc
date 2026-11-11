class PeerError(Exception):
    """The peer broke the protocol"""


class TrackerError(Exception):
    """The tracker didn't answer, refused, or sent something we can't use"""


class TrackerRefused(TrackerError):
    """
    The tracker answered, but refused the announce.
    It's up, and might take announces for other torrents.
    """

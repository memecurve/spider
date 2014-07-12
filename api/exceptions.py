class SpiderException(Exception):
    pass

class MissingParameters(SpiderException):
    pass

class ExtraParameters(SpiderException):
    pass

class StopConsuming(SpiderException):
    pass
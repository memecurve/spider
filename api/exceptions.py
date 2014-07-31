class SpiderException(Exception):
    pass

class MissingParameters(SpiderException):
    pass

class ExtraParameters(SpiderException):
    pass

class MqServiceException(SpiderException):
    pass

class StopConsuming(MqServiceException):
    pass

class HbaseServiceException(SpiderException):
    pass
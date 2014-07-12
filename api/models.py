import json

from api.exceptions import ExtraParameters
from api.exceptions import MissingParameters

class Model(object):

    def __init__(self, *args, **kwargs):
        user_input = set(kwargs.keys())
        missing_fields = self.REQUIRED_FIELDS.difference(user_input)
        extra_fields = user_input.difference(self.REQUIRED_FIELDS.union(self.OPTIONAL_FIELDS))

        if extra_fields:
            raise ExtraParameters("Unrecognized parameters passed to <{0}>: {1}".format(self.__class__.__name__,
                                                                                        ", ".join(extra_fields)))
        if missing_fields:
            raise MissingParameters("Missing a required field(s) in <{0}>: {1}".format(self.__class__.__name__,
                                                                                       ", ".join(missing_fields)))

        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_dict(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self.__dict__)


class Document(Model):
    REQUIRED_FIELDS = {'url', 'markup'}
    OPTIONAL_FIELDS = {'hrefs'}

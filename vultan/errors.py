class CreateIndexError(Exception):
    def __init__(self, collection, indexes):
        self._what = collection.database.name + "." + collection.name
        self._indexes = indexes

    def __repr__(self):
        return "%s %s" % (self._what, self._indexes)

    def __str__(self):
        return self.__repr__()

class DocumentNotFoundError(Exception): pass
class InvalidTransformContextError(Exception): pass
class KeyMatchError(Exception): pass
class MissingAttributeError(Exception): pass
class ScalarUsedInVectorTransformContextError(Exception): pass
class UnrecognizedAttributeError(Exception): pass
class UnsupportedMongodbOpError(Exception): pass

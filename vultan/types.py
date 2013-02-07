import datetime
import pymongo.objectid
import urlparse
import vultan.errors


# Note: must preserve order.
def _unique_list(seq):
    seen = set()
    add = seen.add
    return [item for item in seq if item not in seen and not add(item)]


class Field(object):
    def __init__(self, default=None):
        self._default = default

    def dbname(self, name):
        self._dbname = name
        return self

    def get_dbname(self, name):
        return self._dbname if hasattr(self, '_dbname') else name

    def from_mongo(self, value):
        try:
            return self.do_from_mongo(value)
        except NotImplementedError, exception:
            raise exception
        except:
            return None

    def do_from_mongo(self, value):
        """Unmarshals a mongodb value into a format appropriate for Python.

        This method is expected to be relatively forgiving and should endeavor
        to massage invalid values coming from mongodb into valid Python
        representations. N.B. if do_from_mongo throws an exception, from_mongo
        will suppress the error and simply return None.
        """
        raise NotImplementedError

    def to_mongo(self, value, context='AUTO'):
        if context == 'AUTO' or context == 'NONE':
            return self.do_to_mongo(value)
        if context == 'MANY':
            return [self.do_to_mongo(item) for item in value]
        if context in ['ATOM', 'LIST']:
            raise vultan.errors.ScalarUsedInVectorTransformContextError
        raise vultan.errors.InvalidTransformContextError(context)

    def do_to_mongo(self, value):
        """Marshals a Python value into a format appropriate for mongodb.

        This method should be stricter than do_from_mongo, and in general it
        should reject irreconcilable values by raising an exception.
        """
        return self.do_from_mongo(value)

    def _get_default(self):
        if callable(self._default):
            return self._default()
        return self._default


class IdentityField(Field):
    def do_from_mongo(self, value):
        return value

    def do_to_mongo(self, value):
        return value


class TupleField(Field):
    def __init__(self, *subfields):
        self._subfields = subfields

    def do_from_mongo(self, value):
        if isinstance(value, basestring):
            return None
        try:
            if len(value) != len(self._subfields):
                return None
            return tuple(subfield.from_mongo(item) for subfield, item
                         in zip(self._subfields, value))
        except TypeError:
            return None

    def do_to_mongo(self, value):
        assert isinstance(value, tuple)
        assert len(value) == len(self._subfields)
        return [subfield.to_mongo(item) for subfield, item
                in zip(self._subfields, value)]


class ListField(Field):
    def __init__(self, subfield):
        self._subfield = subfield

    def do_from_mongo(self, value):
        try:
            items = [self._subfield.from_mongo(item) for item in value]
            return filter(lambda x: x != None, items)
        except TypeError:
            return []

    def to_mongo(self, value, context='AUTO'):
        if context in ['LIST', 'MANY']:
            return self.do_to_mongo(value)
        if context == 'ATOM':
            return self._subfield.to_mongo(value)
        if context in ['AUTO', 'NONE']:
            if isinstance(value, list):
                return self.do_to_mongo(value)
            return self._subfield.to_mongo(value)
        raise vultan.errors.InvalidTransformContextError(context)

    def do_to_mongo(self, value):
        if not value:
            return []
        items = [self._subfield.to_mongo(item) for item in value]
        return filter(lambda x: x != None, items)


class SetField(ListField):
    def do_from_mongo(self, value):
        return _unique_list(super(SetField, self).do_from_mongo(value))

    def do_to_mongo(self, value):
        if not value:
            return []
        return super(SetField, self).do_to_mongo(_unique_list(value))


class ObjectIdField(Field):
    def do_from_mongo(self, value):
        if isinstance(value, pymongo.objectid.ObjectId):
            return str(value)
        return None

    def do_to_mongo(self, value):
        if isinstance(value, pymongo.objectid.ObjectId):
            return value
        if isinstance(value, basestring):
            return pymongo.objectid.ObjectId(value)
        assert value is None
        return None


class ObjectIdListField(ListField):
    def __init__(self):
        super(ObjectIdListField, self).__init__(ObjectIdField())


class ObjectIdSetField(SetField):
    def __init__(self):
        super(ObjectIdSetField, self).__init__(ObjectIdField())


class StringField(Field):
    def __init__(self, default=u''):
        self._default = default

    def do_from_mongo(self, value):
        if value == None:
            return self._default
        return unicode(value)

    def do_to_mongo(self, value):
        if value == None:
            return None
        return unicode(value)


class IntField(Field):
    def __init__(self, default=0):
        self._default = default

    def do_from_mongo(self, value):
        try:
            return int(value)
        except TypeError:
            return self._default

    def do_to_mongo(self, value):
        if value == None:
            return None
        return int(value)


class FloatField(Field):
    def __init__(self, default=0.0):
        self._default = default

    def do_from_mongo(self, value):
        try:
            return float(value)
        except TypeError:
            return self._default

    def do_to_mongo(self, value):
        if value == None:
            return None
        return float(value)


class EnumField(Field):
    def __init__(self, enum):
        self._enum = enum

    def do_from_mongo(self, value):
        return value if value in self._enum else None


class EnumSetField(SetField):
    def __init__(self, enum):
        super(EnumSetField, self).__init__(EnumField(enum))
        self._enum = enum

    def do_from_mongo(self, value):
        value = super(EnumSetField, self).do_from_mongo(value)
        return [item for item in self._enum if not value or item in value]

    def do_to_mongo(self, value):
        value = super(EnumSetField, self).do_to_mongo(value)
        return value if len(value) != len(self._enum) else []


class BoolField(Field):
    def __init__(self, default=False):
        self._default = default

    def do_from_mongo(self, value):
        if value == None:
            return self._default
        return bool(value)


class BinaryField(Field):
    def do_from_mongo(self, value):
        if isinstance(value, pymongo.binary.Binary):
            return str(value)
        return None

    def do_to_mongo(self, value):
        return pymongo.binary.Binary(value)


class ObjectField(Field):
    """
    A field that behaves like an object, with named attributes
    that point to vultan.types.Field objects.

    Example usage:
    >>> field = ObjectField(foo=ObjectField(i=IntField(), f=FloatField()),
                            bar=StringField())
    >>> x = field.from_mongo(None)
    >>> x
    {'foo': {'i': 0, 'f': 0.0}, 'bar': u''}
    >>> x.foo.f = 98.6
    >>> x.foo.i = 42
    >>> x.bar = "Hello, world!"
    >>> x.invalid = -1
    >>> x.foo.wrong = 'Oops'
    >>> field.to_mongo(x)
    {'foo': {'i': 42, 'f': 98.599999999999994}, 'bar': u'Hello, world!'}
    >>> field.from_mongo(field.to_mongo(x))
    {'foo': {'i': 42, 'f': 98.599999999999994}, 'bar': u'Hello, world!'}
    """
    class ValueType(dict):
        def __getattr__(self, name):
            try:
                return self[name]
            except KeyError:
                raise AttributeError(name)

        def __setattr__(self, name, value):
            self[name] = value

    def __init__(self, **subfields):
        self._subfields = subfields

    def do_from_mongo(self, value):
        if not isinstance(value, dict):
            value = {}
        answer = self.ValueType()
        for name, subfield in self._subfields.iteritems():
            answer[name] = subfield.from_mongo(value.get(name, None))
        return answer

    def do_to_mongo(self, value):
        answer = {}
        lookup = (lambda k: value.get(k, None)) if isinstance(value, dict) \
            else (lambda k: getattr(value, k, None))
        for name, subfield in self._subfields.iteritems():
            answer[name] = subfield.to_mongo(lookup(name))
        return answer


class DateField(Field):
    def __init__(self, default=None):
        super(DateField, self).__init__(default)

    def do_from_mongo(self, value):
        """Converts a date-like object to a date or returns None."""
        try:
            return datetime.date(value.year, value.month, value.day)
        except AttributeError:
            return self._get_default()

    def do_to_mongo(self, value):
        """Converts a date-like object to a datetime."""
        return datetime.datetime(value.year, value.month, value.day)


class DatetimeField(Field):
    def do_from_mongo(self, value):
        """Returns a datetime object or None."""
        return value if isinstance(value, datetime.datetime) else None

    def do_to_mongo(self, value):
        """Verifies that value is a datetime."""
        if not value:
            return None
        assert isinstance(value, datetime.datetime)
        return value


class UrlField(Field):
    def do_from_mongo(self, value):
        """Returns `value` if it's a valid URL, or None otherwise."""
        result = urlparse.urlparse(value)
        if not result.scheme or not result.netloc:
            return None
        return value

    def do_to_mongo(self, value):
        """If `value` evaluates to false, returns None. Otherwise, returns a
        valid URL or raises a ValueError exception."""
        if not value:
            return None
        transformed = self.do_from_mongo(value)
        if not transformed:
            raise ValueError(value)
        return transformed

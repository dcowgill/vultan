import collections
import pymongo
import pymongo.errors
import vultan.errors
import vultan.types


def _makepair(pair, default):
    if not isinstance(pair, tuple):
        return (pair, default)
    if len(pair) == 1:
        return (pair[0], default)
    if len(pair) == 0:
        return (None, default)
    return pair[0:2]


class Index(object):
    unique = False

    def __init__(self, *args):
        assert len(args) > 0
        self.index = [_makepair(arg, pymongo.ASCENDING) for arg in args]
        self.names = [name for name, _ in self.index]

    @property
    def head(self):
        return self.names[0]

    def match(self, query, unique=False):
        return query

    def __eq__(self, other):
        if not isinstance(other, Index):
            return False
        return self.index == other.index

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.names)


class Key(Index):
    unique = True

    def match(self, query, unique=False):
        if unique:
            for name in self.names:
                assert name in query
        else:
            assert self.head in query
        return query


class _KeySet(object):
    def __init__(self, *args):
        self._keys = collections.defaultdict(list)
        self.names = set()
        self.update(args)
        self.add(Key('id'))

    def add(self, key):
        if key not in self._keys[key.head]:
            self._keys[key.head].append(key)
            for name in key.names:
                self.names.add(name)

    def update(self, keys):
        for key in keys:
            self.add(key)

    def __iter__(self):
        """Iterate over the keys in this set."""
        for keys in self._keys.itervalues():
            for key in keys:
                yield key

    def contains(self, name):
        return name in self.names

    def match(self, query, unique=False):
        """Tries to match any key in this set."""
        for head in query.iterkeys():
            for key in self._keys.get(head, []):
                try:
                    return key.match(query, unique)
                except:
                    pass
        print 'query keys = ', query.keys()
        print 'self._keys = ', self._keys
        raise vultan.errors.KeyMatchError(query)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._keys)


class _Attributes(object):
    def __init__(self, dct):
        self._dct = dct
        if 'id' not in self._dct:
            self._dct['id'] = vultan.types.ObjectIdField().dbname('_id')

    def update(self, attributes):
        for name, fieldtype in attributes:
            if name != 'id':
                assert name not in self._dct
                self._dct[name] = fieldtype

    def get_fieldtype(self, name):
        return self._dct.get(name)

    def get_dbname(self, name):
        fieldtype = self.get_fieldtype(name)
        if not fieldtype:
            return name
        return fieldtype.get_dbname(name)

    def __iter__(self):
        return self._dct.iteritems()


class _DocumentMetaclass(type):
    def __new__(meta, classname, bases, classdict):
        # Rename some keys to indicate that they're "private"
        # for k in ('collection', 'keys', 'attributes'):
        #     try: classdict['_'+k] = classdict.pop(k)
        #     except KeyError: pass

        # Add the base classes' keys to our own
        keys = _KeySet(*classdict.get('_keys', []))
        for base in bases:
            if hasattr(base, '_keys'):
                keys.update(base._keys)
        classdict['_keys'] = keys

        # Add the base classes' attributes to our own
        attrs = _Attributes(classdict.get('_attributes', {}))
        for base in bases:
            if hasattr(base, '_attributes'):
                attrs.update(base._attributes)
        classdict['_attributes'] = attrs

        return type.__new__(meta, classname, bases, classdict)


class ReadOnlyDocument(object):
    __metaclass__ = _DocumentMetaclass

    def __init__(self, data, expected):
        self._data = data
        self._expected = expected
        self._missing = []
        for name, fieldtype in self._attributes:
            self._extract(name, fieldtype)

    def __getattr__(self, name):
        if name in self._missing:
            raise vultan.errors.MissingAttributeError(name)
        raise AttributeError(name)

    def __repr__(self):
        data = self.__dict__.copy()
        for k in data.keys():
            if k.startswith('_'):
                del data[k]
        return '%s(%s)' % (self.__class__.__name__, data)

    @classmethod
    def create_indexes(cls):
        """Ensures that only required indexes exist."""
        created = []
        for key in (key for key in cls._keys if key.names != ['id']):
            key_or_list = [(cls._attributes.get_dbname(name), direction)
                           for name, direction in key.index]
            created.append(cls._get_collection().create_index(
                    key_or_list, unique=key.unique))

        indexes = cls._get_collection().index_information()
        missing = list(set(created) - set(indexes.keys()))
        if missing:
            raise vultan.errors.CreateIndexError(cls._get_collection(), missing)

        dropped = []
        for name, spec in indexes.iteritems():
            if name == '_id_' or (len(spec) == 1 and spec[0][0] == '_id'):
                continue  # ignore the builtin index on "_id"
            if name not in created:
                cls._get_collection().drop_index(name)
                dropped.append(name)

        return (created, dropped)

    #####################
    # protected methods #
    #####################

    @classmethod
    def _get_mongodb(cls):
        return pymongo.Connection('localhost')['test']

    @classmethod
    def _get_collection(cls):
        return cls._get_mongodb()[cls._collection]

    @classmethod
    def _construct(cls, doc, fields):
        return cls(doc, fields)

    @classmethod
    def _make_default(cls, **kwargs):
        data = cls._document_to_mongo(kwargs)
        return cls._construct(doc=data, fields=None)

    @classmethod
    def _find_one(cls, query, fields=None):
        spec = cls._query_to_mongo(cls._keys.match(query, unique=True))
        fields = cls._add_key_fields(fields)
        doc = cls._get_collection().find_one(spec, fields)
        return cls._construct(doc, fields) if doc else None

    @classmethod
    def _find(cls, query, fields=None, skip=0, limit=0, sort=None,
              require_index=True):
        cursor = cls._find_as_cursor(query, fields, skip, limit, require_index)
        if sort:
            cursor.sort(sort)
        return [cls._construct(doc, fields) for doc in cursor]

    @classmethod
    def _exists(cls, query):
        return bool(cls._find_as_cursor(query, limit=1).count())

    @classmethod
    def _count(cls, query):
        return cls._find_as_cursor(query).count()

    ###################
    # private methods #
    ###################

    @classmethod
    def _find_as_cursor(cls, query, fields=None, skip=0, limit=0,
                        require_index=True):
        if require_index:
            cls._keys.match(query)
        return cls._get_collection().find(spec=cls._query_to_mongo(query),
                                          fields=cls._add_key_fields(fields),
                                          skip=skip, limit=limit)

    def _extract(self, name, fieldtype):
        if not self._keys.contains(name):
            if self._expected and name not in self._expected:
                self._missing.append(name)
                return

        value = self._data.get(fieldtype.get_dbname(name))
        setattr(self, name, fieldtype.from_mongo(value))

    @classmethod
    def _add_key_fields(cls, fields):
        if not fields:
            return None
        return list(set(fields).union(cls._keys.names))

    @classmethod
    def _document_to_mongo(cls, doc):
        return cls._to_mongo(doc, 'AUTO')

    @classmethod
    def _query_to_mongo(cls, doc):
        def helper(fieldtype, value):
            if isinstance(value, dict):
                if all(key.startswith('$') for key in value):
                    dct = {}
                    for op, val in value.iteritems():
                        if op in ['$gt', '$lt', '$gte', '$lte', '$ne']:
                            dct[op] = fieldtype.to_mongo(val, 'AUTO')
                        elif op in ['$in', '$nin']:
                            dct[op] = fieldtype.to_mongo(val, 'MANY')
                        else:
                            raise vultan.errors.UnsupportedMongodbOpError(op)
                    return dct
            return fieldtype.to_mongo(value, 'NONE')

        answer = {}
        for key, value in doc.iteritems():
            fieldtype = cls._get_fieldtype(key)
            answer[fieldtype.get_dbname(key)] = helper(fieldtype, value)
        return answer

    @classmethod
    def _update_to_mongo(cls, doc):
        answer = {}
        for key, value in doc.iteritems():
            if key.startswith('$'):
                if key in ['$set', '$unset']:
                    answer[key] = cls._to_mongo(value, 'AUTO')
                elif key in ['$push', '$pull']:
                    answer[key] = cls._to_mongo(value, 'ATOM')
                elif key in ['$pushAll', '$pullAll']:
                    answer[key] = cls._to_mongo(value, 'LIST')
                elif key in ['$inc', '$pop']:
                    answer[key] = cls._to_mongo(value, dotransform=False)
                else:
                    raise vultan.errors.UnsupportedMongodbOpError(key)
            else:
                fieldtype = cls._get_fieldtype(key)
                value = fieldtype.to_mongo(value, 'AUTO')
                answer[fieldtype.get_dbname(key)] = value
        return answer

    @classmethod
    def _to_mongo(cls, doc, context='AUTO', dotransform=True):
        answer = {}
        for key, value in doc.iteritems():
            fieldtype = cls._get_fieldtype(key)
            if dotransform:
                value = fieldtype.to_mongo(value, context)
            answer[fieldtype.get_dbname(key)] = value
        return answer

    @classmethod
    def _get_fieldtype(cls, name):
        fieldtype = cls._attributes.get_fieldtype(name)
        if not fieldtype:
            if '.' in name:
                return vultan.types.IdentityField()
            raise vultan.errors.UnrecognizedAttributeError(name)
        return fieldtype


class Document(ReadOnlyDocument):

    #####################
    # protected methods #
    #####################

    @classmethod
    def _insert(cls, doc):
        """Returns the ObjectId of the inserted document."""
        for key in cls._keys:
            if key.head != 'id':
                key.match(doc, unique=True)
        doc = cls._document_to_mongo(doc)
        object_id = cls._get_collection().insert(doc, safe=True)
        return str(object_id)

    def _insert_multi(cls, docs):
        """Returns the ObjectIds of the inserted documents."""
        for key in cls._keys:
            if key.head != 'id':
                for doc in docs:
                    key.match(doc, unique=True)
        docs = [cls._document_to_mongo(doc) for doc in docs]
        return [str(object_id) for object_id in cls._get_collection().insert(docs, safe=True)]

    @classmethod
    def _update(cls, query, doc, multi=False):
        """Updates exactly one document if multi=False. Otherwise, updates
        zero or more documents."""
        spec = cls._query_to_mongo(cls._keys.match(query, unique=not multi))
        result = cls._get_collection().update(spec, cls._update_to_mongo(doc),
                                              multi=multi, safe=True)
        if not multi and not result['updatedExisting']:
            raise vultan.errors.DocumentNotFoundError(spec)
        return result['n']

    @classmethod
    def _upsert(cls, query, doc):
        """Updates one document or inserts it if it doesn't exist.
           Returns True if it updated an existing document. Otherwise,
           returns False."""
        spec = cls._query_to_mongo(cls._keys.match(query, unique=True))
        result = cls._get_collection().update(spec, cls._update_to_mongo(doc),
                                              multi=False, safe=True, upsert=True)
        return result['updatedExisting']


    @classmethod
    def _remove(cls, query):
        """Removes matching documents."""
        spec = cls._query_to_mongo(cls._keys.match(query))
        result = cls._get_collection().remove(spec, safe=True)
        return result['n']

import collections
import pymongo
import pymongo.errors
import vultan.errors
import vultan.types
from vultan.document import Index, Key, _KeySet, _Attributes


class Storage(object):
    def __init__(self, document_class):
        self._document_class = document_class
        self._keys = document_class._keys
        self._attributes = document_class._attributes

    def create_indexes(self):
        '''Ensures that only required indexes exist.'''
        created = []
        for key in (key for key in self._keys if key.names != ['id']):
            key_or_list = [(self._attributes.get_dbname(name), direction)
                           for name, direction in key.index]
            created.append(self._get_collection().create_index(
                    key_or_list, unique=key.unique))

        indexes = self._get_collection().index_information()
        missing = list(set(created) - set(indexes.keys()))
        if missing:
            raise vultan.errors.CreateIndexError(self._get_collection(), missing)

        dropped = []
        for name, spec in indexes.iteritems():
            if name == '_id_' or (len(spec) == 1 and spec[0][0] == '_id'):
                continue  # ignore the builtin index on "_id"
            if name not in created:
                self._get_collection().drop_index(name)
                dropped.append(name)

        return (created, dropped)

    #
    # protected methods
    #

    def _get_mongodb(self):
        return pymongo.Connection('localhost')['test']

    def _get_collection(self):
        return self._get_mongodb()[self._collection]

    def _construct(self, doc, fields):
        return self._document_class(doc, fields)

    def _make_default(self, **kwargs):
        data = self._document_to_mongo(kwargs)
        return self._construct(doc=data, fields=None)

    def _find_one(self, query, fields=None):
        spec = self._query_to_mongo(self._keys.match(query, unique=True))
        fields = self._add_key_fields(fields)
        doc = self._get_collection().find_one(spec, fields)
        return self._construct(doc, fields) if doc else None

    def _find(self, query, fields=None, skip=0, limit=0, sort=None, require_index=True):
        cursor = self._find_as_cursor(query, fields, skip, limit, require_index)
        if sort:
            cursor.sort(sort)
        return [self._construct(doc, fields) for doc in cursor]

    def _exists(self, query):
        return bool(self._find_as_cursor(query, limit=1).count())

    def _count(self, query, require_index=True):
        return self._find_as_cursor(query, require_index=require_index).count()

    def _insert(self, doc):
        '''Returns the ObjectId of the inserted document.'''
        for key in self._keys:
            if key.head != 'id':
                key.match(doc, unique=True)
        doc = self._document_to_mongo(doc)
        object_id = self._get_collection().insert(doc, safe=True)
        return str(object_id)

    def _update(self, query, doc, multi=False):
        '''Updates exactly one document if multi=False. Otherwise, updates
        zero or more documents.'''
        spec = self._query_to_mongo(self._keys.match(query, unique=not multi))
        result = self._get_collection().update(spec, self._update_to_mongo(doc),
                                               multi=multi, safe=True)
        if not multi and not result['updatedExisting']:
            raise vultan.errors.DocumentNotFoundError(spec)
        return result['n']

    def _upsert(self, query, doc):
        '''Updates one document or inserts it if it doesn't exist.
           Returns True if it updated an existing document. Otherwise
           returns False.'''
        spec = self._query_to_mongo(self._keys.match(query, unique=True))
        result = self._get_collection().update(spec, self._update_to_mongo(doc),
                                               multi=False, safe=True, upsert=True)
        return result['updatedExisting']

    def _remove(self, query):
        '''Removes matching documents.'''
        spec = self._query_to_mongo(self._keys.match(query))
        result = self._get_collection().remove(spec, safe=True)
        return result['n']

    #
    # private methods
    #

    def _find_as_cursor(self, query, fields=None, skip=0, limit=0, require_index=True):
        if require_index: self._keys.match(query)
        return self._get_collection().find(spec=self._query_to_mongo(query),
                                           fields=self._add_key_fields(fields),
                                           skip=skip, limit=limit)

    def _add_key_fields(self, fields):
        if not fields: return None
        return list(set(fields).union(self._keys.names))

    def _document_to_mongo(self, doc):
        return self._to_mongo(doc, 'AUTO')

    def _query_to_mongo(self, doc):
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
            fieldtype = self._get_fieldtype(key)
            answer[fieldtype.get_dbname(key)] = helper(fieldtype, value)
        return answer

    def _update_to_mongo(self, doc):
        answer = {}
        for key, value in doc.iteritems():
            if key.startswith('$'):
                if key in ['$set', '$unset']:
                    answer[key] = self._to_mongo(value, 'AUTO')
                elif key in ['$push', '$pull']:
                    answer[key] = self._to_mongo(value, 'ATOM')
                elif key in ['$pushAll', '$pullAll']:
                    answer[key] = self._to_mongo(value, 'LIST')
                elif key in ['$inc', '$pop']:
                    answer[key] = self._to_mongo(value, dotransform=False)
                else:
                    raise vultan.errors.UnsupportedMongodbOpError(key)
            else:
                fieldtype = self._get_fieldtype(key)
                value = fieldtype.to_mongo(value, 'AUTO')
                answer[fieldtype.get_dbname(key)] = value
        return answer

    def _to_mongo(self, doc, context='AUTO', dotransform=True):
        answer = {}
        for key, value in doc.iteritems():
            fieldtype = self._get_fieldtype(key)
            if dotransform:
                value = fieldtype.to_mongo(value, context)
            answer[fieldtype.get_dbname(key)] = value
        return answer

    def _get_fieldtype(self, name):
        fieldtype = self._attributes.get_fieldtype(name)
        if not fieldtype:
            if '.' in name:
                return vultan.types.IdentityField()
            raise vultan.errors.UnrecognizedAttributeError(name)
        return fieldtype


class SimpleStorage(Storage):
    def find_one(self, **kwargs):
        return self._find_one(kwargs)

    def find(self, **kwargs):
        return self._find(kwargs)

    def exists(self, **kwargs):
        return self._exists(kwargs)

    def count(self, **kwargs):
        return self._count(kwargs)

    def insert(self, **kwargs):
        return self._insert(kwargs)

    def upsert(self, query, **kwargs):
        return self._upsert(query, kwargs)

    def update_one(self, query, **kwargs):
        return self._update(query, kwargs, multi=False)

    def update(self, query, **kwargs):
        return self._update(query, kwargs, multi=True)

    def set_one(self, query, **kwargs):
        return self._update(query, {'$set': kwargs}, multi=False)

    def set(self, query, **kwargs):
        return self._update(query, {'$set': kwargs}, multi=True)

    def remove(self, **kwargs):
        return self._remove(kwargs)


class _NewDocumentMetaclass(type):
    def __new__(meta, classname, bases, classdict):
        # Inherit keys from our bases
        keys = _KeySet(*classdict.get('_keys', []))
        for base in bases:
            if hasattr(base, '_keys'):
                keys.update(base._keys)
        classdict['_keys'] = keys

        # Inherit attributes from our bases
        attrs = _Attributes(classdict.get('_attributes', {}))
        for base in bases:
            if hasattr(base, '_attributes'):
                attrs.update(base._attributes)
        classdict['_attributes'] = attrs

        # Inherit storage from bases unless we have our own
        storage = classdict.get('_storage', classdict.get('_Storage'))
        if not storage:
            for base in bases:
                if hasattr(base, '_storage'):
                    storage = base._storage
                    break

        newtype = type.__new__(meta, classname, bases, classdict)
        newtype.DB = (storage or SimpleStorage)(newtype)
        return newtype


class NewDocument(object):
    __metaclass__ = _NewDocumentMetaclass

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

    #
    # private methods
    #

    def _extract(self, name, fieldtype):
        if not self._keys.contains(name):
            if self._expected and name not in self._expected:
                self._missing.append(name)
                return

        value = self._data.get(fieldtype.get_dbname(name))
        setattr(self, name, fieldtype.from_mongo(value))

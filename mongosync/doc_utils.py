import collections


def doc_flat_to_nested(key_list, val):
    """ Convert a flat keys and value into a nested document.
    e.g.:

    { a.b.c: 1 }

    =>

    { a: { b: { c: 1 } } }
    """
    res = {}
    if len(key_list) > 1:
        res[key_list[0]] = doc_flat_to_nested(key_list[1:], val)
    elif len(key_list) == 1:
        res[key_list[0]] = val
    else:
        raise Exception('invalid key_list @%s' % doc_flat_to_nested.__name__)
    return res


def get_val_by_flat_keys(doc, key_list):
    """ Get value through flat keys from a nested document.
    """
    res = None
    if len(key_list) > 1:
        res = get_val_by_flat_keys(doc[key_list[0]], key_list[1:])
    elif len(key_list) == 1:
        res = doc[key_list[0]]
    else:
        raise Exception('invalid key_list @%s' % get_val_by_flat_keys.__name__)
    return res


def gen_doc_with_fields(doc, include_fields):
    """ Generate document with the specfied fields.
    """
    res = {}
    for f in include_fields:
        try:
            keylist = f.split('.')
            val = get_val_by_flat_keys(doc, keylist)
            nested = doc_flat_to_nested(keylist, val)
            res.update(nested)
        except KeyError:
            pass
    return res


def merge_doc(doc1, doc2):
    """ Merge two documents.
    """
    for k, v in doc2.iteritems():
        if isinstance(v, collections.Mapping):
            doc1[k] = merge_doc(doc1.get(k, {}), v)
        else:
            doc1[k] = v
    return doc1


if __name__ == '__main__':
    doc = {'a': {'b': {'c': 1, 'd': 2}}}
    doc1 = {'a': {'b': {'c': 1}}}
    doc2 = {'a': {'b': {'d': 2}}}

    assert doc_flat_to_nested('a.b.c'.split('.'), 1) == doc1
    assert doc_flat_to_nested('a.b.d'.split('.'), 2) == doc2

    assert get_val_by_flat_keys(doc1, 'a.b.c'.split('.')) == 1
    assert get_val_by_flat_keys(doc2, 'a.b.d'.split('.')) == 2

    assert gen_doc_with_fields(doc, ['a.b.c']) == doc1
    assert gen_doc_with_fields(doc, ['a.b.d']) == doc2

    print 'test cases all pass'

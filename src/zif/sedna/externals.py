"""this is from zope.rdb import parseDSN"""
from urllib import unquote_plus

_dsnFormat = re.compile(
    r"dbi://"
    r"(((?P<username>.*?)(:(?P<password>.*?))?)?"
    r"(@(?P<host>.*?)(:(?P<port>.*?))?)?/)?"
    r"(?P<dbname>.*?)(;(?P<raw_params>.*))?"
    r"$"
    )

_paramsFormat = re.compile(r"([^=]+)=([^;]*);?")

def parseDSN(dsn):
    """Parses a database connection string.

    We could have the following cases:

    dbi://dbname
    dbi://dbname;param1=value...
    dbi://user/dbname
    dbi://user:passwd/dbname
    dbi://user:passwd/dbname;param1=value...
    dbi://user@host/dbname
    dbi://user:passwd@host/dbname
    dbi://user:passwd@host:port/dbname
    dbi://user:passwd@host:port/dbname;param1=value...

    Any values that might contain characters special for URIs need to be
    quoted as it would be returned by `urllib.quote_plus`.

    Return value is a mapping with the following keys:

    username     username (if given) or an empty string
    password     password (if given) or an empty string
    host         host (if given) or an empty string
    port         port (if given) or an empty string
    dbname       database name
    parameters   a mapping of additional parameters to their values
    """

    if not isinstance(dsn, (str, unicode)):
        raise ValueError('The dsn is not a string. It is a %r' % type(dsn))

    match = _dsnFormat.match(dsn)
    if match is None:
        raise ValueError('Invalid DSN; must start with "dbi://": %r' % dsn)

    result = match.groupdict("")
    raw_params = result.pop("raw_params")

    for key, value in result.items():
        result[key] = unquote_plus(value)

    params = _paramsFormat.findall(raw_params)
    result["parameters"] = dict([(unquote_plus(key), unquote_plus(value))
                                for key, value in params])

    return result
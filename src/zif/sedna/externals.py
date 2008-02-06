"""
ZPL 2.1
Zope Public License (ZPL) Version 2.1
A copyright notice accompanies this license document that identifies the copyright holders.
This license has been certified as open source. It has also been designated as GPL compatible by the Free Software Foundation (FSF).
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 Redistributions in source code must retain the accompanying copyright notice, this list of conditions, and the following disclaimer.
 Redistributions in binary form must reproduce the accompanying copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
 Names of the copyright holders must not be used to endorse or promote products derived from this software without prior written permission from the copyright holders.
 The right to distribute this software or to use it for any purpose does not give you the right to use Servicemarks (sm) or Trademarks (tm) of the copyright holders. Use of them is covered by separate agreement with the copyright holders.
 If any files are modified, you must cause the modified files to carry prominent notices stating that you changed the files and the date of any change.
Disclaimer
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

"""parseDSN from zope.rdb"""
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
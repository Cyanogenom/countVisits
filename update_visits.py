import re
import time

from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError

from http import server
import psycopg2

from raven import Client
import logging



class HTTPServer(server.BaseHTTPRequestHandler):
    _raven_dsn = '_dsn_'
    _client = Client(_raven_dsn)

    _re_product_id = re.compile(r'products/(\d+)[/\?]?')
    _prof_id = '162086698'
    _step = 1000

    _POSTGRES_DB_HOST = 'localhost'
    _POSTGRES_DB_NAME = 'data'
    _POSTGRES_DB_USER = 'postgres'
    _POSTGRES_DB_PASSWORD = 'pass'
    _POSTGRES_DB_TABLE_NAME = 'visits'
    _POSTGRES__DB_PORT = 5432

    _correct_path = '/update_visits'

    def _get_top_keywords(self, service, start_idx):

        return service.data().ga().get(
            ids='ga:' + self._prof_id,
            start_date='182daysAgo',
            end_date='today',
            metrics='ga:pageViews',
            dimensions='ga:pagePath',
            start_index=start_idx,
            filters='ga:pagePath=@/products/',
        ).execute()

    def _parse_results(self, rows):
        d = {}
        for row in rows:
            if self._re_product_id.findall(row[0]):
                d_id = self._re_product_id.findall(row[0])[0]
                if d.get(d_id, None):
                    d[d_id] += int(row[1])
                else:
                    d[d_id] = int(row[1])

        return d

    def _logging(self, type, path, headers, error, xid):
        d = {
            'type': type,
            'path': path,
            'headers': headers,
            'time': time.time(),
            'error': error
        }
        if xid:
            d['X-Request-Id'] = xid
        self._client.captureMessage(d)
        logging.error(d)


    def do_GET(self):
        _xid = self.headers['X-Request-Id']

        code = 200
        message = 'ok'
        if self.path == self._correct_path:
            service, flags = sample_tools.init(
                [], 'analytics', 'v3', __doc__, __file__,
                scope='https://www.googleapis.com/auth/analytics.readonly')

            try:
                idx = 1
                total = 0
                rows = []
                while idx == 1 or idx < total:
                    results = self._get_top_keywords(service, idx)
                    if total == 0:
                        total = results['totalResults']
                    if results.get('rows', []):
                        rows += results['rows']
                    else:
                        break
                    idx += self._step

                d = self._parse_results(rows)
                error = self._send_data_to_db(d)
                if error:
                    code = 500
                    message = 'Error'
                    self._logging('ERROR', self.path, str(self.headers), 'There was an error with db: %s' % error, _xid)

            except TypeError as error:
                code = 500
                message = 'Error'
                # Handle errors in constructing a query.
                self._logging('ERROR', self.path, self.headers,
                              'There was an error in constructing your query : %s' % error, _xid)

            except HttpError as error:
                code = 500
                message = 'Error'
                # Handle API errors.
                self._logging('ERROR', self.path, self.headers,
                              'Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason()), _xid)

            except AccessTokenRefreshError:
                code = 500
                message = 'Error'
                # Handle Auth errors.
                self._logging('ERROR', self.path, self.headers, 'The credentials have been revoked or expired, '
                                                       'please re-run the application to re-authorize', _xid)

        elif self.path == '/healthcheck':
            code = 200
        else:
            code = 404
            message = 'Error'

        self.send_response(code, message)
        self.end_headers()

    def _send_data_to_db(self, data):
        conn_string = "host=%s port=%d dbname=%s user=%s password=%s" % (self._POSTGRES_DB_HOST, self._POSTGRES__DB_PORT,
                                                                         self._POSTGRES_DB_NAME, self._POSTGRES_DB_USER,
                                                                         self._POSTGRES_DB_PASSWORD)
        try:
            db = psycopg2.connect(conn_string)
            cur = db.cursor()
            query = 'INSEsRT INTO %s (id, num) VALUES' % self._POSTGRES_DB_TABLE_NAME
            for elem in data.keys():
                query += '(%s, %s),' % (elem, data[elem])
            query = query[:-1] + 'ON CONFLICT (id) DO UPDATE SET num=EXCLUDED.num;'
            cur.execute(query)
            db.commit()
            cur.close()
            db.close()
        except psycopg2.OperationalError as error:
            return error
        except psycopg2.ProgrammingError as error:
            return error

        return None

if __name__ == '__main__':
    URL = 'localhost'
    LISTEN_PORT = 1234
    serv = server.HTTPServer((URL, LISTEN_PORT), HTTPServer)
    serv.serve_forever()
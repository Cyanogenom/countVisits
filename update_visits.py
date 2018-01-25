import re

from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError

import http.server
import psycopg2


re_product_id = re.compile(r'products/(\d+)[/\?]?')
prof_id = 'id'
step = 1000


def get_top_keywords(service, start_idx):

    return service.data().ga().get(
        ids='ga:' + prof_id,
        start_date='182daysAgo',
        end_date='today',
        metrics='ga:pageViews',
        dimensions='ga:pagePath',
        start_index=start_idx,
        filters='ga:pagePath=@/products/',
    ).execute()


def parse_results(rows):
    d = {}
    for row in rows:
        if len(re_product_id.findall(row[0])) != 1:
            print(row, re_product_id.findall(row[0]))

        if re_product_id.findall(row[0]):
            d_id = re_product_id.findall(row[0])[0]
            if d.get(d_id, None):
                d[d_id] += int(row[1])
            else:
                d[d_id] = int(row[1])


    return d


class HTTPServer(http.server.BaseHTTPRequestHandler):
    host = 'localhost'
    db_name = 'data'
    user = 'postgres'
    password = 'pass'
    table_name = 'visits'

    def do_GET(self):
        code = 200
        service, flags = sample_tools.init(
            [], 'analytics', 'v3', __doc__, __file__,
            scope='https://www.googleapis.com/auth/analytics.readonly')

        # Try to make a request to the API. Print the results or handle errors.
        try:
            idx = 1
            total = 0
            rows = []
            while idx == 1 or idx < total:
                print(idx)
                results = get_top_keywords(service, idx)
                if total == 0:
                    total = results['totalResults']
                if results.get('rows', []):
                    rows += results['rows']
                else:
                    break
                idx += step

            d = parse_results(rows)
            self.send_data_to_db(d)
            print(len(d.keys()))

        except TypeError as error:
            code = 400
            # Handle errors in constructing a query.
            print(('There was an error in constructing your query : %s' % error))

        except HttpError as error:
            code = 400
            # Handle API errors.
            print(('Arg, there was an API error : %s : %s' %
                   (error.resp.status, error._get_reason())))

        except AccessTokenRefreshError:
            code = 400
            # Handle Auth errors.
            print('The credentials have been revoked or expired, please re-run '
                  'the application to re-authorize')

        self.send_response(code)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes('ok'.encode('utf-8')))


    def send_data_to_db(self, data):
        conn_string = "host=%s port=%d dbname=%s user=%s password=%s" % (self.host, 5432, self.db_name, self.user, self.password)
        db = psycopg2.connect(conn_string)
        cur = db.cursor()
        query = 'DELETE FROM %s;' % self.table_name
        cur.execute(query)
        print(cur.statusmessage)
        query = 'INSERT INTO %s (id, num) VALUES' % self.table_name
        for elem in data.keys():
            query += '(%s, %s),' % (elem, data[elem])
        query = query[:-1] + ';'
        print(query)
        cur.execute(query)
        print(cur.statusmessage)
        db.commit()
        cur.close()
        db.close()





if __name__ == '__main__':
    serv = http.server.HTTPServer(('localhost', 1234), HTTPServer)
    serv.serve_forever()
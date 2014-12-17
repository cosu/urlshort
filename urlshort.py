__author__ = 'cdumitru'

import random
import string
import requests
from pymongo import MongoClient
from threading import Thread
import ConfigParser, os
import time


TASK_COUNT = 10000
GOOGLE_API_KEY = ''

BACKOFF = 0
SUCCESS_BACKOFF = 0

# db.urls.find({"longUrl" : {$regex : "pass"}}).sort({"created":-1})

def gen():
    size = 5
    chars = string.ascii_uppercase + string.digits + string.ascii_lowercase
    TASK_COUNT = 1000
    i = 0

    while i < TASK_COUNT:
        i += 1
        yield ''.join(random.choice(chars) for _ in range(size))


def get(collection):
    global BACKOFF
    global SUCCESS_BACKOFF

    token = next(gen())

    while token:
        url = 'https://www.googleapis.com/urlshortener/v1/url?shortUrl=http://goo.gl/%s&projection=FULL&key=%s' % (
        token, GOOGLE_API_KEY)

        r = requests.get(url)

        if r.status_code < 300:
            response = r.json()
            collection.insert(response)
            if response['status'] == 'OK':
                print token, response['longUrl']
            if BACKOFF > 1:
                SUCCESS_BACKOFF += 1

            if SUCCESS_BACKOFF > 10:
                BACKOFF = 0
                SUCCESS_BACKOFF = 0


        else:
            print r.text

            if r.status_code == 403:
                BACKOFF += 1
                print "backoff %s" % BACKOFF

        time.sleep(BACKOFF)
        token = next(gen())


if __name__ == '__main__':
    client = MongoClient()
    db = client.URLShort
    url_collection = db.urls

    config = ConfigParser.ConfigParser()
    config.read('urlshort.cfg')
    GOOGLE_API_KEY = config.get('keys', 'GOOGLE_API_KEY')

    for i in range(8):
        t = Thread(target=get, args=(url_collection,))
        t.start()

        # for _ in range(TASK_COUNT):
        #     get(url_collection)


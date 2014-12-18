from Queue import Queue
from collections import Counter

__author__ = 'cdumitru'

import random
import string
import requests
from pymongo import MongoClient
from threading import Thread
import ConfigParser
import logging
import time
import signal
import sys
import threading

GOOGLE_API_KEY = None
SLEEP_DELAY = 0

lock = threading.Lock()


def gen():
    """
    Generates random <size> strings containing lower, upper and digits
    :return: the string
    :rtype: str


    """
    size = 6
    chars = string.ascii_uppercase + string.digits + string.ascii_lowercase
    while True:
        yield ''.join(random.choice(chars) for _ in range(size))


def inc_delay():
    global SLEEP_DELAY
    with lock:
        SLEEP_DELAY += random.randint(1, 10)
        logger.debug("increased sleep to %s" % SLEEP_DELAY)


def dec_delay():
    global SLEEP_DELAY
    with lock:
        SLEEP_DELAY -= random.randint(1, 3)

        if SLEEP_DELAY < 0:
            SLEEP_DELAY = 0
        logger.debug("decreased sleep to %s" % SLEEP_DELAY)


def get_and_insert(collection, token):
    """
    Resolves the short url and if successful saves the result to mongo
    :param collection: the collection where to save the restul
    :param token: the url token to resolve
    :return: the HTTP status code of the get operation. If > 200 then it was unsuccessful
    """

    url = 'https://www.googleapis.com/urlshortener/v1/url?shortUrl=http://goo.gl/%s&projection=FULL&key=%s' % (
    token, GOOGLE_API_KEY)

    r = requests.get(url, timeout=103)

    if r.status_code < 300:
        response = r.json()
        collection.insert(response)
        if response['status'] == 'OK':
            logger.debug(" %s: %s " % (token, response['longUrl']))

    else:

        if r.status_code != 404:
            logger.debug("Error resolving %s : %s" % (url, r.text))

    return r.status_code


def worker(token_queue, collection, id):
    """
    Main worker loop. In case of API errors the worker backs off and sleeps. The sleep interval is reduced only if
    the requests no longer fail.
    :param token_queue: the queue from where the tokens are read by each worker
    :param collection: the mongodb collection where the result is saved
    :param id: the id of the worker
    :return: nothing
    """
    counter = Counter()

    while True:
        token = queue.get()

        counter['total'] += 1

        code = get_and_insert(collection, token)

        if code == 403:
            inc_delay()

        if code == 200 or code == 404:
            if SLEEP_DELAY > 0:
                dec_delay()
        else:
            queue.put(token)

        counter[code] += 1

        time.sleep(SLEEP_DELAY)

        token_queue.task_done()

        if counter['total'] % 100 == 0:
            logger.debug("%i %s" % (id, counter))


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)

if __name__ == '__main__':

    # config stuff
    config = ConfigParser.ConfigParser()
    config.read('urlshort.cfg')
    GOOGLE_API_KEY = config.get('keys', 'GOOGLE_API_KEY')

    # logging
    logger = logging.getLogger("urlshort")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


    #mongo
    client = MongoClient()
    db = client.URLShort
    url_collection = db.urls

    # signal
    signal.signal(signal.SIGTERM, signal_handler)


    #queue

    queue = Queue()

    for _ in range(10000):
        queue.put(next(gen()))

    for id in range(20):
        t = Thread(target=worker, args=(queue, url_collection, id))
        t.daemon = True
        t.start()

    queue.join()



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
import threading


TOKEN_LENGTH = 6

URL_COUNT = 10000  # number of URLS to resolve
THREAD_COUNT = 20  # number of threads to start

GOOGLE_API_KEY = None
SLEEP_DELAY = 0

lock = threading.Lock()


def gen(size):
    """
    Generates random <size> strings containing lower, upper and digits
    :param: the length of the random string
    :type: int
    :return: the string
    :rtype: str
    """
    chars = string.ascii_uppercase + string.digits + string.ascii_lowercase
    while True:
        yield ''.join(random.choice(chars) for _ in range(size))


def inc_delay():
    """
    increases the global delay counter with a random value
    :return: nothing
    """
    global SLEEP_DELAY
    with lock:
        SLEEP_DELAY += random.randint(1, 10)
        logger.debug("increased sleep to %s" % SLEEP_DELAY)


def dec_delay():
    """
    decreases the global delay counter with a smaller random value
    :return: nothing
    """
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
    :param
    :param id: the id of the worker
    :type: int
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


if __name__ == '__main__':

    # config stuff if no key provided
    if not GOOGLE_API_KEY:
        config = ConfigParser.ConfigParser()
        config.read('urlshort.cfg')
        GOOGLE_API_KEY = config.get('keys', 'GOOGLE_API_KEY')

    # logging
    logger = logging.getLogger("urlshort")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # mongo
    client = MongoClient()
    db = client.URLShort
    url_collection = db.urls

    # queue

    queue = Queue()

    for _ in range(URL_COUNT):
        queue.put(next(gen(TOKEN_LENGTH)))

    for tid in range(THREAD_COUNT):
        t = Thread(target=worker, args=(queue, url_collection, tid))
        t.daemon = True
        t.start()

    queue.join()



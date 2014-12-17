from Queue import Queue

__author__ = 'cdumitru'

import random
import string
import requests
from pymongo import MongoClient
from threading import Thread
import ConfigParser
import logging
import time

GOOGLE_API_KEY = None

# db.urls.find({"longUrl" : {$regex : ""}}).sort({"created":-1})

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


def get_and_insert(collection, token):
    """
    Resolves the short url and if successful saves the result to mongo
    :param collection: the collection where to save the restul
    :param token: the url token to resolve
    :return: the HTTP status code of the get operation. If > 200 then it was unsuccessful
    """

    url = 'https://www.googleapis.com/urlshortener/v1/url?shortUrl=http://goo.gl/%s&projection=FULL&key=%s' % (
    token, GOOGLE_API_KEY)

    r = requests.get(url)

    if r.status_code < 300:
        response = r.json()
        collection.insert(response)
        if response['status'] == 'OK':
            logger.debug(" %s: %s " % (token, response['longUrl']))

    else:

        if r.status_code != 404:
            logger.debug("Error resolving %s : %s" % (url, r.text))

    return r.status_code


def worker(token_queue, collection):
    """
    Main worker loop. In case of API errors the worker backs off and sleeps. The sleep interval is reduced only if
    the requests no longer fail.
    :param token_queue: the queue from where the tokens are read by each worker
    :param collection: the mongodb collection where the result is saved
    :return: nothing
    """
    retry_sleep = 0
    success_backoff = 0
    while True:
        token = queue.get()
        # rate limit
        code = get_and_insert(collection, token)
        if code == 403:
            # increment sleep

            retry_sleep += 1
            logger.debug("increased sleep to %s" % retry_sleep)

        if code == 200:
            if retry_sleep > 0:
                success_backoff += 1
                if success_backoff > 10:
                    success_backoff = 0
                    retry_sleep = 0

        if code != 200:
            queue.put(token)

        time.sleep(retry_sleep)

        token_queue.task_done()



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


    #queue

    queue = Queue()

    for _ in range(10000):
        queue.put(next(gen()))

    for _ in range(8):
        t = Thread(target=worker, args=(queue, url_collection))
        t.daemon = True
        t.start()

    queue.join()



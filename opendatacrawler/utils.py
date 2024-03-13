
import os
from url_normalize import url_normalize
from w3lib.url import url_query_cleaner
import configparser
import pathlib
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from setup_logger import logger

def check_url(url):
    """ Check if exist a well-formed url"""
    if url[:8] == "https://" or url[:7] == "http://":
        return True
    else:
        return False
    
def create_folder(path):
    if not os.path.isdir(path):
        try:
            os.mkdir(path)
        except OSError:
            print("Creation of the dir %s failed" % path)
            return False
        else:
            print("Successfully created the dir %s " % path)
            return True
    else:
        return True
    

def clean_url(u):
    """Clean a url string to obtain the mainly domain without protocols."""

    u = url_normalize(u)
    parameters = ['utm_source',
                  'utm_medium',
                  'utm_campaign',
                  'utm_term',
                  'utm_content']

    u = url_query_cleaner(u, parameterlist=parameters,
                          remove=True)

    if u.startswith("http://"):
        u = u[7:]
    if u.startswith("https://"):
        u = u[8:]
    if u.startswith("www."):
        u = u[4:]
    if u.endswith("/"):
        u = u[:-1]
    return u.split('/')[0]


def get_token(domain):
    config = configparser.ConfigParser()
    current_path = pathlib.Path(__file__).parent.resolve()
    config.read(str(current_path) + '/config.ini')
    return config.get(domain,'token')

# to do: faltar implementar que sucede cuando la id es None 
def generate_hash(string, id):
    combined_data = f"{string}{id}"
    sha256_hash = hashlib.sha256()
    sha256_hash.update(combined_data.encode('utf-8'))
    id_hash = sha256_hash.hexdigest()

    return id_hash


def save_temporal_ids(path, ids):
    with open(path, "a") as file:
        for id in ids:
            file.write("{}\n".format(id))


def get_difference(new_ids, last_ids):
    new_ids_set = set(new_ids)
    last_ids_set = set(last_ids)

    return list(new_ids_set-last_ids_set)

def delete_interrupted_files(path):
    try:
        os.remove(path)
        logger.info('Removed {}'.format(path))
    except:
        logger.info('Could not remove {}'.format(path))




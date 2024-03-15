import os
import requests
import utils
import re
import time
import json
from setup_logger import logger
from ZenodoCrawler import ZenodoCrawler
from DataEuropaCrawler import DataEuropaCrawler
import urllib3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures.thread
from tqdm import tqdm
from threading import Event
import signal
from sys import exit

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



class OpenDataCrawler():

    def __init__(self, domain, path, formats=None, sec=None):
        
        if domain[-1]=="/":
            self.domain = domain[:-1]
        else: self.domain = domain
        self.dms = None
        self.dms_instance = None
        self.formats = formats
        self.max_sec = 60
        self.last_ids = None

        # Save path or create one based on selected domain. Create selected dms directory.
        if not path:
            path = os.getcwd()+ '/' + utils.clean_url(self.domain)
            utils.create_folder(path)
            self.save_path = path

        else: 
            self.save_path = path + '/' + utils.clean_url(self.domain)
            utils.create_folder(self.save_path)
        
        self.resume_path = self.save_path + "/resume_{}.txt".format(utils.clean_url(self.domain))

        print('Detecting DMS')
        # Detect dms based on domain.
        self.detect_dms()
    
    def detect_dms(self):
        
        # Check dms
          
        dms = dict([
            ('Zenodo','/api/records/'),
            ('DataEuropa', '/api/hub/search/')])

        for key, value in dms.items():
            try:
                response = requests.get(self.domain+value, verify=False)
                if response.status_code == 200 and response.headers['Content-Type']!="text/html":
                    self.dms = key
                    print('DMS detected')
                    logger.info("DMS detected %s", key)
                    # Create data and metadata directories.
                    data_path = self.save_path + '/data'
                    metadata_path = self.save_path + '/metadata'
                    if not utils.create_folder(data_path):
                        logger.info("Can't create folder" + data_path)
                        exit()
                    if not utils.create_folder(metadata_path):
                        logger.info("Can't create folder" + metadata_path)
                        exit()

                    self.last_ids = self.get_last_ids()
                    
                    break

            except Exception as e:
                logger.info(e)
        
        # Create an instance of the corresponding dms.
                
        if (self.dms):
            if self.dms=='Zenodo':
                self.dms_instance = ZenodoCrawler(self.dms, self.formats)
            if self.dms=='DataEuropa':
                self.dms_instance = DataEuropaCrawler(self.dms, self.formats)
        else:
            print("The domain " + self.domain + " is not supported yet")
            logger.info("DMS not detected in %s", self.domain)
        
        
    # Generic method for saving a resource from an url. 
    def save_dataset(self, url, ext, id):
        """ Save a dataset from a given url and extension"""
        try:
            # Web page is not consideret a dataset
            if url[-4:] != 'html':

                logger.info("Saving... %s ", url)

                with requests.get(url, stream=True, timeout=60, verify=False) as r:

                    # Checks if response content is not a webpage.
                    response_content_type = r.headers.get('Content-Type')
                    is_html = 'text/html' in response_content_type
                    
                    if r.status_code == 200 and not is_html:
                        fname = id + '.' + ext 
                        path = self.save_path + "/data" + "/"+ fname
                        total_size = int(r.headers.get('content-length', 0))
                        # Write the content on a file
                        with open(path, 'wb') as outfile, tqdm(desc=fname, total=total_size, colour='green',unit='B' ,unit_scale=True, unit_divisor=1024, leave=False) as bar:
                            partial = False
                            start_time = time.time()
                            for chunk in r.iter_content(chunk_size=1024):
                                # Stops file download if max download time is reached.
                                if self.max_sec and ((time.time() - start_time) > self.max_sec):
                                    partial = True
                                    logger.warning('Timeout! Partially downloaded file: {}/{}'.format(url, id))
                                    break

                                if chunk:
                                    size = outfile.write(chunk)
                                    outfile.flush()
                                    bar.update(size)
                                    
                        if not partial:
                            logger.info("Dataset saved from {}/{}".format(url, id))
                            return (id, path, partial)
                        else:
                            #utils.delete_interrupted_files(path)
                            return (id, path, partial)
                        
                    else:
                        logger.warning('Problem obtaining the resource %s', url)

                        return (id, None, False)
                    
        except Exception as e:
            logger.error('Error saving dataset from %s', url)
            logger.error(e)
            return (id, None, False)
    
    def save_partial_dataset():
        return None
    
    def save_metadata(self, data):

        """ Save the dict containing the metadata on a json file"""
        try:
            with open(self.save_path + '/metadata' + "/meta_" + data['custom_id'] + '.json',
                      'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error('Error saving metadata  %s',
                         self.save_path + "/meta_"+data['custom_id']+'.json')
            logger.error(e)

    def get_package_list(self):
        return self.dms_instance.get_package_list()

    def get_package(self, id):
        return self.dms_instance.get_package(id)

    # Downloads and saves package resources. 
    def get_package_resources(self, package):
        resources = package['resources']
        downloaded_resources = []
        updated_resources = []
        urls = []
        ids = []
        mediatypes = []
        
        # Filters resources by format if specified. Selects all resources otherwise.
        if self.formats:
            for resource in resources:
                format = resource['mediatype']
                if format and format in self.formats:
                    downloaded_resources.append(resource)
                else:
                    resource['path'] = None
                    updated_resources.append(resource)
        else:
            downloaded_resources = resources

        # Downloads and saves selected resources.
        if len(downloaded_resources) > 0:
            urls = [resource['download_url'] for resource in downloaded_resources]
            ids = [resource['resource_id'] for resource in downloaded_resources]
            mediatypes = [resource['mediatype'] for resource in downloaded_resources]
            results = []

            # Parallel download of resources.
            with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks to the executor.
                futures = [executor.submit(self.save_dataset, url, mediatype, id) for url, mediatype, id in zip(urls, mediatypes, ids)]
                
                # Wait for all tasks to complete.
                try:
                    for future in as_completed(futures):
                        try:
                            results.append(
                                future.result()
                                )
                        except Exception as e:
                            print(f"An error occurred: {e}")
                except KeyboardInterrupt:
                    executor._threads.clear()
                    concurrent.futures.thread._threads_queues.clear()
                    for id, mediatype in zip(ids, mediatypes):
                            path = self.save_path + '/data' + '/{}.{}'.format(id, mediatype)
                            utils.delete_interrupted_files(path)
                    logger.info("Keyboard interruption!")
                    exit()

            # Metadata for each downloaded resource is updated.
            for result in results:
                try:
                    current_id = result[0]
                    current_path = result[1]
                    is_partial = result[2]
                    current_resource = [resource for resource in downloaded_resources if resource['resource_id']==current_id][0]

                    if current_path != None and not is_partial :
                        current_resource['path'] = current_path
                    elif current_path != None:          
                        current_resource['path'] = None
                        # Removes file if it was partially downloaded due to a timeout.
                        utils.delete_interrupted_files(current_path)
                    else:
                        current_resource['path'] = None

                    updated_resources.append(current_resource)
                except Exception as e:
                    logger.error(e)

            #if any(any(value != None for value in d.values()) for d in updated_resources)
            package['resources'] = updated_resources
            utils.save_temporal_ids(self.resume_path, [package['id']])
            return package
        else:
            utils.save_temporal_ids(self.resume_path, [package['id']])
            return package



    def get_last_ids(self):
        saved_ids = []
        if os.path.exists(self.resume_path):
            with open(self.resume_path, "r") as file:
                for line in file:
                    saved_ids.append(line.strip())
            return saved_ids
        else:
            return None





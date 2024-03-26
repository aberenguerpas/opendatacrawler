import os
import requests
from opendatacrawler import utils
import time
import json
import urllib3
import time
from opendatacrawler import setup_logger
from opendatacrawler.portals.ZenodoCrawler import ZenodoCrawler
from opendatacrawler.portals.DataEuropaCrawler import DataEuropaCrawler
from tqdm import tqdm
from sys import exit

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = setup_logger.create_logger()

class OpenDataCrawler():
    def __init__(self, domain, path, formats=None, only_metadata=False):
        
        if domain[-1]=="/":
            self.domain = domain[:-1]
        else: self.domain = domain
        self.dms = None
        self.dms_instance = None
        self.formats = formats
        self.max_sec = 60
        self.last_ids = None
        self.only_metadata = only_metadata

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
                if self.formats:
                    formats = []
                    for format in self.formats:
                        if format in DataEuropaCrawler.get_formats_dict():
                            formats.extend(DataEuropaCrawler.get_formats_dict().get(format))
                        else:
                            formats.append(format)
                    self.formats = formats
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

                    # Tries to get resource format in case it is None
                    if ext is None:
                        if url.split('.')[-1] in self.formats:
                            ext = url.split('.')[-1]
                        elif response_content_type.split('/')[-1] in self.formats:
                            ext = response_content_type.split('/')[-1]
                        else:
                            response_split = response_content_type.split(';')
                            for split in response_split:
                                if split.split('/')[-1] in self.formats:
                                    ext = split.split('/')[-1]
                   
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
                        logger.warning('Problem obtaining the resource {}'.format(url))

                        return (id, None, False)

        except KeyboardInterrupt:
            raise
                   
        except Exception as e:
            logger.error('Error saving dataset from %s', url)
            logger.error(e)
            return (id, None, False)
        
    
    def save_partial_dataset():
        return None
    
    def save_metadata(self, data):

        """ Save the dict containing the metadata on a json file"""
        try:
            with open(self.save_path + '/metadata' + "/meta_" + data['custom:id'] + '.json',
                      'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error('Error saving metadata  %s',
                         self.save_path + "/meta_"+data['custom:id']+'.json')
            logger.error(e)

    def get_package_list(self):
        return self.dms_instance.get_package_list()

    def get_package(self, id):
        return self.dms_instance.get_package(id)

    # Downloads and saves package resources. 
    def get_package_resources(self, package):
        resources = package['dcat:distribution']
        downloaded_resources = []
        updated_resources = []
        
        # Filters resources by format if specified. Selects all resources otherwise.
        if self.formats:
            for resource in resources:
                format = resource['dcat:mediaType']
                if format and format in self.formats:
                    downloaded_resources.append(resource)
                else:
                    resource['custom:path'] = None
                    updated_resources.append(resource)
        else:
            downloaded_resources = resources

        # Downloads and saves selected resources.
        if len(downloaded_resources) > 0:
            for resource in downloaded_resources:
                url = resource['dcat:downloadURL']
                id = resource['custom:resource_id']
                mediatype = resource['dcat:mediaType']

                result = self.save_dataset(url, mediatype, id)
                current_path = result[1]
                is_partial = result[2]
                
                if current_path != None and not is_partial :
                    resource['custom:path'] = current_path
                elif current_path != None:          
                    # Removes file if it was partially downloaded due to a timeout.
                    utils.delete_interrupted_files(current_path)
                
                updated_resources.append(resource)

        package['dcat:distribution'] = updated_resources
        utils.save_temporal_ids(self.resume_path, [package['dct:identifier']])
        
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

    def process_package(self, id):
        try:
            package = self.get_package(id)
            if package and not self.only_metadata:
                updated_package = self.get_package_resources(package)
                if updated_package:
                    self.save_metadata(updated_package)
            elif package:
                self.save_metadata(package)
                utils.save_temporal_ids(self.resume_path, [package['dct:identifier']])
        except KeyboardInterrupt:
            raise
            
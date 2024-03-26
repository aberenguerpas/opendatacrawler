from opendatacrawler.portals.crawler_interface_abc import OpenDataCrawlerInterface
from opendatacrawler import utils
import requests
import time
from opendatacrawler import setup_logger
from tqdm import tqdm

class ZenodoCrawler(OpenDataCrawlerInterface):
    def __init__(self, domain, formats):
        self.domain = domain
        self.formats = formats
        self.token = utils.get_token(domain)
    
    # Collects ids from all packages.
    def get_package_list(self):
        url = 'https://zenodo.org/api/records?q=&page={}&size=200&resource_type=dataset{}'
        formats = []
        logger = setup_logger.create_logger()

        if self.formats:
            formats = ['&file_type={}'.format(format) for format in self.formats]

        stop_condition = False
        page = 1
        ids = []
        result_count = []

        try:
            while not stop_condition:
                response = requests.get(url.format(page,"".join(formats)))
                if response.status_code == 200:
                    if page==1:
                        result_count = response.json().get('hits').get('total')
                        pbar = tqdm(total = int(result_count/200), bar_format='{desc}: {percentage:3.0f}%|{bar}')
                    datasets = response.json()['hits']['hits']
                    for dataset in datasets:
                        ids.append(dataset['id'])
                    next_page = response.json().get('links').get('next', None)
                    # Checks pagination.
                    if not next_page:
                        stop_condition = True
                    else:
                        page+=1
                    pbar.update(1)
                # If API call limit is reached, waits 60s to make next call (Zenodo limit is 60 requests per minute, 2000 requests per hour).
                else:
                    logger.info(response.status_code)
                    time.sleep(60)
            return ids
        
        except Exception as e:
             logger.error(e)
        
    # to do: falta gestionar el límite de llamadas en la API de Zenodo.
    
    def get_package(self, id):
        url = 'https://zenodo.org/api/records/{}'.format(id)
        try:
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()
            hash_id = utils.generate_hash(self.domain, id)
            resources = []
            
            # Initialize metadata dict.
            package_data = {
                        'id': id,
                        'custom_id': hash_id,
                        'title': None,
                        'description': None,
                        'theme': None,
                        'issued': None,
                        'modified': None,
                        'license': None,
                        'country': None,
                        'resources': None
                    }
            
            # Check metadata existence and update dict.
            if response_json.get('metadata'):
                metadata = response_json.get('metadata')
                if metadata.get('title'):
                    package_data['title'] = response_json.get('title')
                if metadata.get('description'):
                    package_data['description'] = response_json.get('description')
                if metadata.get('keywords'):
                    package_data['theme'] = response_json.get('keywords')
            
            if response_json.get('created'):
                package_data['issued'] = response_json.get('created')
            
            if response_json.get('modified'):
                package_data['modified'] = response_json.get('modified')
            
            if response_json.get('license'):
                if response_json.get('license').get('id'):
                    package_data['license'] = response_json.get('license').get('id')

            # Save resources metadata
            if response_json.get('files'):
                for resource in response_json.get('files'):
                    format = None if resource.get('key') is None else resource.get('key').split('.')[-1]
                    resources.append({
                        'download_url': None if resource.get('links') is None else resource.get('links').get('self', None),
                        'resource_id': utils.generate_hash(self.domain, resource.get('id', None)),
                        'mediatype': format.lower() if format else None 
                    })
            package_data['resources'] = resources
            return package_data
        except requests.exceptions.HTTPError as errh:
            print(f"HTTP Error: {errh}")



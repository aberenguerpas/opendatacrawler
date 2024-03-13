from crawler_interface_abc import OpenDataCrawlerInterface
import configparser
import utils
import requests
import sys
from setup_logger import logger
from tqdm import tqdm

class DataEuropaCrawler(OpenDataCrawlerInterface):

    base_url = 'https://data.europa.eu/api/hub/search/'

    def __init__(self, domain, formats):
        self.domain = domain
        self.formats = formats
    
    
    
    # Retrieves ids from all datasets.
    def get_package_list(self):
        url = """search?q=&filter=dataset&limit=1000&page={}&sort=relevance%2Bdesc,+modified%2Bdesc,+
        title.en%2Basc&facetOperator=OR&facetGroupOperator=OR&dataServices=false&includes=id,title.en,description.en,languages,modified,
        issued,catalog.id,catalog.title,catalog.country.id,distributions.id,distributions.format.label,distributions.format.id,distributions.
        license,categories.label,publisher"""
        ids = []
        formats = []
        result_count = []
        page = 1
        stop_condition = False

        if self.formats:
            formats = ["%22" + format.upper() + "%22" for format in self.formats]
        
        try:
            while not stop_condition:
                response = requests.get(DataEuropaCrawler.base_url+url.format(page)+
                                        "&facets=%7B%22format%22:[{}]%7D".format(",".join(formats)))
                if response.status_code==200:
                    data = response.json().get('result')
                    if len(data.get('results'))!=0:
                        if page == 1:
                            result_count = data.get('count')
                            pbar = tqdm(total = int(result_count/1000), bar_format='{desc}: {percentage:3.0f}%|{bar}')
                        datasets = data.get('results')
                        for dataset in datasets:
                            ids.append(dataset.get('id'))
                        page+=1
                        pbar.update(1)
                    else:
                        stop_condition = True
                else:
                     stop_condition = True

            pbar.close()
            return ids
        
        except Exception as e:
             logger.error(e)
        


    # Retrieves and processes package/dataset metadata.
    def get_package(self, id):
            url = DataEuropaCrawler.base_url + 'datasets/{}'.format(id)
            try:
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()['result']
                hash_id = utils.generate_hash(self.domain, id)
                filtered_resources = []
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
                if response_json.get('title'):
                    if response_json.get('title').get('es'):
                          package_data['title'] = response_json.get('title').get('es')
                    elif response_json.get('title').get('en'):
                         package_data['title'] = response_json.get('title').get('en')
                
                if response_json.get('description'):
                    if response_json.get('description').get('es'):
                          package_data['description'] = response_json.get('description').get('es')
                    elif response_json.get('description').get('en'):
                         package_data['description'] = response_json.get('description').get('en')
            
                if response_json.get('license'):
                     if response_json.get('license').get('id'):
                          package_data['license'] = response_json.get('license').get('id')

                if response_json.get('country'):
                     if response_json.get('country').get('label'):
                          package_data['country'] = response_json.get('country').get('label')

                package_data['theme'] = response_json.get('theme-taxonomy', None)
                package_data['issued'] = response_json.get('issued', None)
                package_data['modified'] = response_json.get('modified', None)

                # Save resources metadata.
                if response_json.get('distributions'):
                     for resource in response_json.get('distributions'):
                          format = None if resource.get('format') is None else resource.get('format').get('id', None)
                          resources.append({
                               'download_url': resource.get('access_url')[0] if resource.get('access_url')[0] else None,
                               'resource_id': utils.generate_hash(self.domain, resource.get('id', None)),
                               'mediatype': format.lower() if format else None                               
                          })
                
                package_data['resources'] = resources

                return package_data
            
            except requests.exceptions.HTTPError as errh:
                    print(f"HTTP Error: {errh}")


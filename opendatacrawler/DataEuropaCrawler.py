from crawler_interface_abc import OpenDataCrawlerInterface
import configparser
import utils
import requests
import sys
from setup_logger import logger
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures.thread
from sys import exit

class DataEuropaCrawler(OpenDataCrawlerInterface):

    base_url = 'https://data.europa.eu/api/hub/search/'

    def __init__(self, domain, formats):
        self.domain = domain
        self.formats = formats



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
                     'url': None,
                     'title': None,
                     'description': None,
                     'theme': None,
                     'keywords': None,
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
                
                if response_json.get('categories'):
                    categories = []
                    for category in response_json.get('categories'):
                        if category.get('label'):
                            label = category.get('label')
                            if label.get('es'):
                                 categories.append({
                                      'language': 'es',
                                      'label': label.get('es')
                                 })
                            if label.get('en'):
                                 categories.append({
                                      'language': 'en',
                                      'label': label.get('en')
                                 })
                    package_data['theme'] = categories 
                
                if response_json.get('keywords'):
                    keywords = []
                    for keyword in response_json.get('keywords'):
                        if keyword.get('language'):
                            language = keyword.get('language')
                            if language in ['es','en']:
                                    keywords.append({
                                        'language': language,
                                        'label': keyword.get('label')
                                    })
                    package_data['keywords'] = keywords
                          
                            
                package_data['issued'] = response_json.get('issued', None)
                package_data['modified'] = response_json.get('modified', None)
                package_data['url'] = response_json.get('resource', None)

                # Save resources metadata.
                if response_json.get('distributions'):
                    for resource in response_json.get('distributions'):
                        format = None if resource.get('format') is None else resource.get('format').get('id', None)
                        if resource.get('access_url')[0]:
                            access_url =  resource.get('access_url')[0]
                            if '&compressed=true' in access_url:
                                download_url = access_url.replace('&compressed=true', '')
                            else: 
                                download_url = access_url
                        else:
                            download_url = None

                        resources.append({
                            'download_url': download_url,
                            'resource_id': utils.generate_hash(self.domain, resource.get('id', None)),
                            'mediatype': format.lower() if format else None                               
                        })
                
                package_data['resources'] = resources

                return package_data
            
            except requests.exceptions.HTTPError as errh:
                    print(f"HTTP Error: {errh}")


    # Retrieves ids from all datasets.
    def get_package_list(self):

        ids = []
        params = {}
        q_w = ""        
        url = 'https://data.europa.eu/sparql'
                
        # Total datasets
        if self.formats:
            format_text = " || ".join([f"CONTAINS(LCASE(STR(?format)),'{f}')" for f in self.formats])
            q_w = """
             where{
                ?dataset a <http://www.w3.org/ns/dcat#Dataset> .
                ?dataset <http://www.w3.org/ns/dcat#distribution> ?distribution .
                ?distribution <http://purl.org/dc/terms/format> ?format .
                FILTER ("""+format_text+")}"
        else:
            q_w = 'where{?dataset a <http://www.w3.org/ns/dcat#Dataset>'
        
        params = {
                'query': 'select (count( distinct ?dataset) as ?total)' + q_w
        }

        header = {
            'Accept': 'application/sparql-results+json'
        }
        res = requests.get(url, params=params, headers=header)
        
        total = int(res.json()['results']['bindings'][0]['total']['value'])
        
        
        # Retrieve ids
        for offset in tqdm(range(0,total,50000), total=total):
            params = {
                'query': 'select distinct ?dataset '+q_w+' LIMIT 50000 OFFSET '+ str(offset)
            }
            header = {
                'Accept': 'application/sparql-results+json'
            }
            res = requests.get(url, params=params, headers=header)
            
            ids.extend(list(map(lambda dataset: dataset['dataset']['value'].split("/")[-1], res.json()['results']['bindings'])))
        return ids
    
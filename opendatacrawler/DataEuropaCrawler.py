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
        url = """search?q=&filter=dataset&limit=1000&page={}&sort=relevance%2Bdesc,+modified%2Bdesc,+
        title.en%2Basc&facetOperator=OR&facetGroupOperator=OR&dataServices=false&includes=id,title.en,description.en,languages,modified,
        issued,catalog.id,catalog.title,catalog.country.id,distributions.id,distributions.format.label,distributions.format.id,distributions.
        license,categories.label,publisher"""
        ids = []
        formats = []
        result_count = []

        if self.formats:
            formats = ["%22" + format.upper() + "%22" for format in self.formats]
        
        # Checks number of results.
        response = requests.get(DataEuropaCrawler.base_url+url.format(1)+
                                    "&facets=%7B%22format%22:[{}]%7D".format(",".join(formats)))
        data = response.json().get('result')
        result_count = data.get('count')

        # Estimates number of pages.
        total_pages = int(result_count/1000) + 1
        pages_list = range(1, total_pages)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.get_ids_page, url, page, formats) for page in pages_list]
            pbar = tqdm(total = total_pages, bar_format='{desc}: {percentage:3.0f}%|{bar}')
            try:
                 for future in as_completed(futures):
                      result = future.result()
                      ids.extend(result)
                      pbar.update(1)
            except Exception as e:
                print(e)
        
        return ids
    
    # Retrieves ids from page.
    def get_ids_page(self, url, page, formats):
        ids = []
        response = requests.get(DataEuropaCrawler.base_url+url.format(page)+
                                        "&facets=%7B%22format%22:[{}]%7D".format(",".join(formats)))
        if response.status_code==200:
            data = response.json().get('result')
            if len(data.get('results'))!=0:
                datasets = data.get('results')
                for dataset in datasets:
                     ids.append(dataset.get('id'))
            
        return ids

    
    def pages_iter(self, page_count):
        page_chunks = []

        for i in range(1, page_count+1, 5):
            group = list(range(i, min(i+5, page_count+1)))
            page_chunks.append(group)
        
        return page_chunks
        

   

    

        

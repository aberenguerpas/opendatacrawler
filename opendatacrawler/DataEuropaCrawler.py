import utils
import requests
from crawler_interface_abc import OpenDataCrawlerInterface
from tqdm import tqdm

class DataEuropaCrawler(OpenDataCrawlerInterface):
    base_url = 'https://data.europa.eu/api/hub/search/'
    formats_dict = {
         'csv':['csv','text/csv','.csv','csv/utf8','file:///srv/udata/ftype/csv'],
         'xlsx':['xlsx','xls','.xlsx','.xls','excel (.xlsx)'],
         'pdf':['pdf','.pdf','application/pdf']
    }

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
                     'publisher': None,
                     'language': None,
                     'issued': None,
                     'modified': None,
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

                if response_json.get('country'):
                          package_data['country'] = response_json['country'].get('label', None)
                          
                if response_json.get('language'):
                          package_data['language'] = response_json['language'][0].get('label', None)
                          
                if response_json.get('catalog'):
                     if response_json.get('catalog').get('publisher'):
                          package_data['publisher'] = {}
                          package_data['publisher']['name'] = response_json.get('catalog').get('publisher').get('name', None)
                          package_data['publisher']['homepage'] = response_json.get('catalog').get('publisher').get('homepage', None)
                
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
                package_data['url'] = response_json.get('resource', None).replace("88u/dataset","data/datasets")

                # Save resources metadata.
                if response_json.get('distributions'):
                    if len(response_json['distributions']) > 0:
                        for resource in response_json.get('distributions'):
                            format = None if resource.get('format') is None else resource.get('format').get('id', None)
                            if resource.get('download_url'):
                                download_url = resource.get('download_url')[0]
                            elif resource.get('access_url'):
                                access_url =  resource.get('access_url')[0]
                                if '&compressed=true' in access_url:
                                    download_url = access_url.replace('&compressed=true', '')
                                else: 
                                    download_url = access_url
                            else:
                                download_url = None
                            
                            license = None 
                            
                            if resource.get('rights'):
                                license = resource.get('rights').get('resource', None)
                            
                            if resource.get('license'):
                                license = resource.get('license').get('label', None)
                            
                            resources.append({
                                'download_url': download_url,
                                'resource_id': utils.generate_hash(self.domain, resource.get('id', None)),
                                'mediatype': format.lower() if format else None,
                                'license': license,
                                'path': None                            
                            })
                    else:
                    # No resources, maybe link to web with the info
                        if package_data.get('landing_page'):
                            for landing in package_data.get('landing_page'):
                                resources.append({
                                    'download_url':  landing['resource'],
                                    'resource_id': None,
                                    'mediatype': 'Web',
                                    'license': None,
                                    'resource_id': None,
                                    'title': list(landing['title'].keys())[0]
                                    
                                })
                
                package_data['resources'] = resources

                return package_data
            except Exception as e:
                print(e)
            #except requests.exceptions.HTTPError as errh:
            #        print(f"HTTP Error: {errh}")
                return None


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
            q_w = 'where{?dataset a <http://www.w3.org/ns/dcat#Dataset>}'
        
        params = {
                'query': 'select (count( distinct ?dataset) as ?total)' + q_w
        }
        
        header = {
            'Accept': 'application/sparql-results+json'
        }
        res = requests.get(url, params=params, headers=header)
        
        total = int(res.json()['results']['bindings'][0]['total']['value'])
        
        
        # Retrieve ids
        for offset in tqdm(range(0,total,50000), bar_format='{desc}: {percentage:3.0f}%|{bar}'):
            params = {
                'query': 'select distinct ?dataset '+q_w+' LIMIT 50000 OFFSET '+ str(offset)
            }
            header = {
                'Accept': 'application/sparql-results+json'
            }
            res = requests.get(url, params=params, headers=header)
            
            ids.extend(list(map(lambda dataset: dataset['dataset']['value'].split("/")[-1], res.json()['results']['bindings'])))
        return ids
    
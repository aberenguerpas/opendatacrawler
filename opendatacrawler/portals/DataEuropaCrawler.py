import requests
import urllib.parse
from opendatacrawler.utils import utils
from opendatacrawler.portals.crawler_interface_abc import OpenDataCrawlerInterface
from tqdm import tqdm

class DataEuropaCrawler(OpenDataCrawlerInterface):
    base_url = 'https://data.europa.eu/api/hub/search/'
    
    def __init__(self, domain, formats):
        self.domain = domain
        self.formats = formats

    # Retrieves and processes package/dataset metadata.
    def get_formats_dict():
        return {
         'csv':['csv','text/csv','.csv','csv/utf8','file:///srv/udata/ftype/csv'],
         'xlsx':['xlsx','xls','.xlsx','.xls','excel (.xlsx)'],
         'pdf':['pdf','.pdf','application/pdf']
        }  
        
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
                     'dct:identifier': id,
                     'custom:id': hash_id,
                     'custom:url': None,
                     'dct:title': None,
                     'dct:description': None,
                     'dcat:theme': None,
                     'dcat:keyword': None,
                     'dct:publisher': None,
                     'dct:language': None,
                     'dct:issued': None,
                     'dct:modified': None,
                     'custom:country': None,
                     'dcat:distribution': None
                }

                # Check metadata existence and update dict.
                if response_json.get('title'):
                    if response_json.get('title').get('es'):
                          package_data['dct:title'] = {
                               'language':'es',
                               'label':response_json.get('title').get('es')
                          }
                    elif response_json.get('title').get('en'):
                         package_data['dct:title'] = {
                               'language':'en',
                               'label':response_json.get('title').get('en')
                          }
                    else:
                        titles = response_json.get('title')
                        t_keys = list(titles.keys())
                        if len(t_keys)>0:
                            package_data['dct:title'] = {
                                 'language':t_keys[0],
                                 'label':titles.get(t_keys[0])
                            }
                
                if response_json.get('description'):
                    if response_json.get('description').get('es'):
                          package_data['dct:description'] = {
                               'language':'es',
                               'label':response_json.get('description').get('es')
                          }
                    elif response_json.get('description').get('en'):
                         package_data['dct:description'] = {
                              'language':'en',
                              'label':response_json.get('description').get('en')
                         }
                    else:
                         descriptions = response_json.get('description')
                         d_keys = list(descriptions.keys())
                         if len(d_keys)>0:
                              package_data['dct:description'] = {
                                   'language':d_keys[0],
                                   'label':descriptions.get(d_keys[0])
                              }
                         

                if response_json.get('country'):
                          package_data['custom:country'] = response_json['country'].get('label', None)
                          
                if response_json.get('language'):
                          package_data['dct:language'] = response_json['language'][0].get('label', None)
                          
                if response_json.get('publisher', None):
                    package_data['dct:publisher'] = {}
                    package_data['dct:publisher']['name'] = response_json.get('publisher',{}).get('name', None)
                    package_data['dct:publisher']['homepage'] = response_json.get('publisher',{}).get('resource', None)
        
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
                            elif label.get('en'):
                                 categories.append({
                                      'language': 'en',
                                      'label': label.get('en')
                                 })
                            elif len(list(label.keys()))>0:
                                 categories.append({
                                      'language': list(label.keys())[0],
                                      'label': label.get(list(label.keys())[0])
                                 })
                                
                    package_data['dcat:theme'] = categories 
                
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
                    if len(keywords) == 0:
                         keywords = [{
                              'language': keyword.get('language'),
                              'label': keyword.get('label')
                         } for keyword in response_json.get('keywords') ]
                    package_data['dcat:keyword'] = keywords
                          
                            
                package_data['dct:issued'] = response_json.get('issued', None)
                package_data['dct:modified'] = response_json.get('modified', None)
                package_data['custom:url'] = response_json.get('resource', None).replace("88u/dataset","data/datasets")

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
                            
                            resource_size = resource.get('byte_size', None)
                            
                            resource_title = None

                            if resource.get('title'):
                                resource_titles = resource.get('title')
                                if resource_titles.get('es'):
                                    resource_title = {
                                        'language':'es',
                                        'label':resource_titles.get('es') 
                                    }
                                elif resource_titles.get('en'):
                                    resource_title = {
                                         'language':'en',
                                         'label':resource_titles.get('en')
                                    }
                                else:
                                    t_r_keys = list(resource_titles.keys())
                                    if len(t_r_keys)>0:
                                         resource_title = {
                                              'language':t_r_keys[0],
                                              'label':resource_titles.get(t_r_keys[0])
                                         }
                                                                     
                            
                            resources.append({
                                'dct:title': resource_title,
                                'dcat:downloadURL': urllib.parse.unquote(download_url),
                                'custom:resource_id': utils.generate_hash(self.domain, resource.get('id', None)),
                                'dcat:mediaType': format.lower() if format else None,
                                'dcat:byteSize': resource_size,
                                'dct:rights': license,
                                'custom:path': None                            
                            })
                    else:
                    # No resources, maybe link to web with the info
                        if package_data.get('landing_page'):
                            for landing in package_data.get('landing_page'):
                                resources.append({
                                    'dcat:downloadURL':  landing['resource'],
                                    'custom:resource_id': None,
                                    'dcat:mediaType': 'Web',
                                    'dct:rights': None,
                                    'dct:title': list(landing['title'].keys())[0]
                                    
                                })
                
                package_data['dcat:distribution'] = resources

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
    
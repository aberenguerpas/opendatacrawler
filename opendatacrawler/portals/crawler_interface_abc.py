from abc import abstractmethod
from abc import ABCMeta

class OpenDataCrawlerInterface(metaclass=ABCMeta):
    @abstractmethod
    def get_package_list():
        """ This funciton must be used to obtain a list of all packages ids
            from the portal.

            return ids: list
        """
        pass

    @abstractmethod
    def get_package(id):
        """ This function must be used to obtain the metadata from a given
            package id and also format the metadata with the following
            structure inside a dict:

            metadata['id_portal'] -> Packages identifier
            metadata['id'] -> Portal identifier
            metadata['title'] -> Packages title

            metadata['description'] -> Packages Description
            metadata['language'] -> Data language (ES spanish, EN english)
            metadata['theme'] -> Packages category, theme, topic...
            metadata['resources'] -> List of resources
                            resource['name'] -> Resource name
                            resource['mediaType'] -> Type of resource Ex. csv, pdf..
                            resource['size'] -> Resource size
                            resource['downloadUrl'] -> Resource url to download
            metadata['modified'] -> Last modification of the package
            metadata['issued'] -> Creation date of the package
            metadata['license'] -> Package license
            metadata['source'] -> Package source(domain)
            metadata['source_name] -> Domain name
            metadata['temporal_coverage']
                            coverage['start_date'] -> Starting date of the data
                            coverage['end_date'] -> Finishing date of the data
            metadata['spatial_coverage'] -> Country, city, town, ...

            return metadata: dict
        """
        pass
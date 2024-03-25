from setuptools import setup, find_packages

setup(
    name='opendatacrawler',
    version='0.1',
    packages=find_packages(),
    description='An open crawler for Open data portals',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Alberto Berenguer Pastor, Javier Ríos Cerdán',
    author_email='alberto.berenguer@ua.es',
    install_requires=[
        'certifi==2021.10.8',
        'charset-normalizer==2.0.12',
        'feedparser==6.0.8',
        'html2text==2020.1.16',
        'idna==3.3',
        'mccabe==0.6.1',
        'numpy==1.26.4',
        'pyflakes==2.4.0',
        'requests==2.27.1',
        'sgmllib3k==1.0.0',
        'six==1.16.0',
        'sodapy==2.1.0',
        'tqdm==4.63.0',
        'url-normalize==1.4.3',
        'urllib3==1.26.8',
        'w3lib==1.22.0'
    ],
)
import argparse
import utils
import os
from setup_logger import logger
import traceback
from tqdm import tqdm
from odcrawler import OpenDataCrawler


def main():

    parser = argparse.ArgumentParser()

    # Arguments

    parser.add_argument('-d', '--domain', type=str, required=True)
    parser.add_argument('-p', '--path', type=str, required=False)
    parser.add_argument('-m','--save_meta', required=False, action=argparse.BooleanOptionalAction)
    parser.add_argument('-f', '--formats', nargs='+', required=False)


    args = vars(parser.parse_args())

    # Save arguments to variables

    url = args['domain']
    path = args['path']
    save_meta = args['save_meta']
    formats = list(map(lambda x: x.lower(), args['formats'])) if args['formats'] else None

    

    # Main script

    try:
        if (utils.check_url(url)):
            crawler = OpenDataCrawler(domain=url, path=path, formats=formats)

            if crawler.dms:
                logger.info("Obtaining packages from %s", url)
                print("Obtaining packages from " + url)
                packages_ids = crawler.get_package_list()
                logger.info("%i packages found", len(packages_ids))
                print(str(len(packages_ids)) + " packages found!")

                # Checks for previous downloaded packages.
                if crawler.last_ids:
                    package_difference = utils.get_difference(packages_ids, crawler.last_ids)
                    packages_ids = package_difference
                    print('Previous download detected! ({} packages left)'.format(len(package_difference)))
                    print('Resuming...')
                else:
                    print('Previous download not detected.')

                # Saves resources and metadata for each package.
                if packages_ids:
                    for id in tqdm(packages_ids, desc="Processing", colour="green"):
                        logger.info('STARTING PACKAGE {}'.format(id))
                        package = crawler.get_package(id)

                        if package:
                            updated_package = crawler.get_package_resources(package)
                            if save_meta and updated_package:
                                crawler.save_metadata(updated_package)

                    # Removes resume_data if all resources were succesfully saved.
                    os.remove(crawler.resume_path)
                else: 
                    print("Error ocurred while obtaining packages!")


        else:
            print("Incorrect domain form.\nMust have the form "
                "https://domain.example or http://domain.example")
    
    except Exception:
        print(traceback.format_exc())
        print('Keyboard interrumption!')


if __name__ == "__main__":
    main()
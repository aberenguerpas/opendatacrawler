from utils import utils
from utils import setup_logger
from tqdm import tqdm
from portals.odcrawler import OpenDataCrawler
from concurrent.futures import ThreadPoolExecutor, as_completed
from sys import exit
import argparse
import os
import traceback

def main():
    logger = setup_logger.logger

    # Arguments
    parser = argparse.ArgumentParser()

    # Arguments
    parser.add_argument('-d', '--domain', type=str, required=True)
    parser.add_argument('-p', '--path', type=str, required=False)
    parser.add_argument('-f', '--formats', nargs='+', required=False,
                        help='Filter which resources will be downloaded by format (csv, xlsx, pdf, zip)')
    parser.add_argument('-m','--metadata', required=False, action=argparse.BooleanOptionalAction,
                        help='Only save metadata.')
  
    args = vars(parser.parse_args())

    # Save arguments to variables
    url = args['domain']
    path = args['path']
    only_metadata = args['metadata']
    formats = list(
        map(lambda x: x.lower(), args['formats'])) if args['formats'] else None

    # Main script
    try:
        if (utils.check_url(url)):
            crawler = OpenDataCrawler(domain=url, path=path, formats=formats, only_metadata=only_metadata)

            if crawler.dms:
                logger.info("Obtaining packages from %s", url)
                print("Obtaining packages from " + url)
                packages_ids = crawler.get_package_list()
                logger.info("%i packages found", len(packages_ids))
                print(str(len(packages_ids)) + " packages found!")

                # Checks for previous downloaded packages.
                if crawler.last_ids:
                    package_difference = utils.get_difference(
                        packages_ids, crawler.last_ids)
                    packages_ids = package_difference
                    print('Previous download detected! ({} packages left)'.format(
                        len(package_difference)))
                    print('Resuming...')
                else:
                    print('Previous download not detected.')

                # Saves resources and metadata for each package.
                if packages_ids:
                    pbar = tqdm(total = len(packages_ids))
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        try:
                            futures = [executor.submit(crawler.process_package, id) for id in packages_ids]
                            for _ in as_completed(futures):
                                pbar.update(1)
                        except KeyboardInterrupt:
                            print('\nWaiting for left tasks to complete!')
                            logger.info("Keyboard interruption!")
                            logger.info('Waiting for left tasks to complete!')
                            executor.shutdown(wait=False, cancel_futures=True)
                            exit()
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

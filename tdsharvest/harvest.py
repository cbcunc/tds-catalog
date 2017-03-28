"""
Harvest ISO metadata from NetCDF catalogued by THREDDS.
"""

import sys
import os
import argparse
import requests
import time
import logging
from thredds_crawler.crawl import Crawl

LOG_FORMAT = "%(levelname)s: %(asctime)s: pid=%(process)d: %(message)s"
SELECTORS = [".*\.nc$", ".*\.ncml$"]
SERVICES = ["ISO"]
FILE_MODE = 0o755
EXTENSION = ".xml"
WRITE_MODE = "w"
CRAWL_CATALOG_EXIT_STATUS = 1
REAP_MKDIRS_ISOPATH_EXIT_STATUS = 2
CRAWL_RESULTS_REMOVE_EXIT_STATUS = 3
REAP_RETRY_REMOVE_EXIT_STATUS = 4
REAP_RETRY_WRITE_EXIT_STATUS = 5


def parseargs():
    """Parse the command line arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument("catalog",
                        help="The URL of the catalog.xml to harvest.")
    parser.add_argument("isopath",
                        help="The path in which to place the ISO metadata.")
    parser.add_argument("logpath",
                        help="The path of the log file.")
    parser.add_argument("crawl_results_path",
                        help="The path of the crawl results file.")
    parser.add_argument("reap_retry_path",
                        help="The path of the reap retry file.")
    return parser.parse_args()


def crawl(catalog):
    """Crawl the THREDDS catalog for NetCDF."""

    try:
        c = Crawl(catalog, select=SELECTORS)
    except:
        logging.error("Error crawling catalog = %s", catalog, exc_info=True)
        print("Exit due to error crawling catalog")
        sys.exit(CRAWL_CATALOG_EXIT_STATUS)
    indices = [(dataset.id, dataset.services) for dataset in c.datasets]
    return [(id_, service["url"])
            for id_, services in indices
            for service in services
            if service["service"] in SERVICES]


def reap(isopath, resources):
    """Reap the ISO metadata from the results of crawling the catalog."""

    errors = []
    try:
        os.makedirs(isopath, mode=FILE_MODE, exist_ok=True)
    except:
        logging.critical("Error making isopath directory = %s",
                         isopath,
                         exc_info=True)
        print("Exit due to error making isopath directory.")
        sys.exit(REAP_MKDIRS_ISOPATH_EXIT_STATUS)
    for resource, isourl in resources:
        logging.info("resource = %s", resource)
        logging.info("url = %s", isourl)
        resource_dir, resource_name = os.path.split(resource)
        isodir = os.path.join(isopath, resource_dir)
        try:
            os.makedirs(isodir, mode=FILE_MODE, exist_ok=True)
        except:
            logging.error("Error making isodir directory = %s",
                          isodir,
                          exc_info=True)
            errors.append(isourl)
        else:
            try:
                response = requests.get(isourl)
            except:
                logging.error("Error getting isourl = %s",
                              isourl,
                              exc_info=True)
                errors.append(isourl)
            else:
                xmlpath = os.path.join(isodir, resource_name+EXTENSION)
                try:
                    with open(xmlpath, WRITE_MODE) as iso:
                        iso.write(response.text)
                except:
                    logging.error("Error writing xmlpath = %s",
                                  xmlpath,
                                  exc_info=True)
                    errors.append(isourl)
                else:
                    logging.info("metadata = %s", xmlpath)
        print(".", end="")
    return errors

if __name__ == "__main__":
    args = parseargs()
    logging.basicConfig(filename=args.logpath,
                        level=logging.DEBUG,
                        format=LOG_FORMAT)
    logging.info("Beginning harvest.")
    try:
        if os.path.exists(args.crawl_results_path):
            os.remove(args.crawl_results_path)
    except:
        logging.error("Error removing crawl results file", exc_info=True)
        print("Exit due to error removing crawl results file.")
        sys.exit(CRAWL_RESULTS_REMOVE_EXIT_STATUS)
    try:
        if os.path.exists(args.reap_retry_path):
            os.remove(args.reap_retry_path)
    except:
        logging.error("Error removing reap retry file", exc_info=True)
        print("Exit due to error removing reap retry file.")
        sys.exit(REAP_RETRY_REMOVE_EXIT_STATUS)
    resources = crawl(args.catalog)
    logging.info("Crawl complete for catalog = %s", args.catalog)
    if resources:
        results = ["name = {0}\nurl = {1}\n".format(*resource)
                   for resource in resources]
        try:
            with open(args.crawl_results_path, WRITE_MODE) as crawl_results:
                crawl_results.writelines(results)
        except:
            logging.error("Error writing crawl results file", exc_info=True)
            print("Exit due to error writing crawl results file.")
            sys.exit(CRAWL_RESULTS_WRITE_EXIT_STATUS)
        errors = reap(args.isopath, resources)
        if errors:
            errors = [error + "\n" for error in errors]
            try:
                with open(args.reap_retry_path, WRITE_MODE) as reap_retry:
                    reap_retry.writelines(errors)
            except:
                logging.error("Error writing reap retry file", exc_info=True)
                print("Exit due to error writing reap retry file.")
                sys.exit(REAP_RETRY_WRITE_EXIT_STATUS)
    logging.info("Harvest complete.")

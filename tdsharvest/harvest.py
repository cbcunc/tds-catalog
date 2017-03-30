#! /bin/env python3
"""
Harvest ISO metadata from NetCDF catalogued by THREDDS.

usage: python harvest.py [-h]
                         catalog isopath logpath crawl_results_path
                         reap_retry_path reap_success_path

positional arguments:
  catalog             The URL of the catalog.xml to harvest.
  isopath             The path in which to place the ISO metadata.
  logpath             The path of the log file.
  crawl_results_path  The path of the crawl results file.
  reap_retry_path     The path of the reap retry file.
  reap_success_path   The path of the success retry file.

optional arguments:
  -h, --help          show this help message and exit
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
REAP_MKDIRS_ISOPATH_EXIT_STATUS = 2
CRAWL_RESULTS_REMOVE_EXIT_STATUS = 3


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
    parser.add_argument("reap_success_path",
                        help="The path of the success retry file.")
    return parser.parse_args()


def crawl(catalog):
    """Crawl the THREDDS catalog for NetCDF."""

    try:
        c = Crawl(catalog, select=SELECTORS)
    except:
        # Crawler failed: quit program.
        logging.error("Error crawling catalog = %s", catalog, exc_info=True)
        print("Exit due to error crawling catalog", file=sys.stderr)
        sys.exit(CRAWL_CATALOG_EXIT_STATUS)
    indices = [(dataset.id, dataset.services) for dataset in c.datasets]
    return [(id_, service["url"])
            for id_, services in indices
            for service in services
            if service["service"] in SERVICES]


def reap(isopath, resources):
    """Reap the ISO metadata from the results of crawling the catalog."""

    errors, successes = [], []
    try:
        os.makedirs(isopath, mode=FILE_MODE, exist_ok=True)
    except:
        # Could not make directory for all metadata: quit program.
        logging.critical("Error making isopath directory = %s",
                         isopath,
                         exc_info=True)
        print("Exit due to error making isopath directory.", file=sys.stderr)
        sys.exit(REAP_MKDIRS_ISOPATH_EXIT_STATUS)
    else:
        # Sucess making directory for all metadata.
        for resource, isourl in resources:
            # For each resource found by the crawler, create metadata.
            logging.info("resource = %s", resource)
            logging.info("url = %s", isourl)
            resource_dir, resource_name = os.path.split(resource)
            isodir = os.path.join(isopath, resource_dir)
            try:
                os.makedirs(isodir, mode=FILE_MODE, exist_ok=True)
            except:
                # Could not make directory for metadata resource:
                # log and keep looping for next resource.
                logging.error("Error making isodir directory = %s",
                              isodir,
                              exc_info=True)
                errors.append(isourl)
                continue
            else:
                # Success making directory for metadata resource.
                try:
                    response = requests.get(isourl)
                except:
                    # Could not fetch metadata:
                    # log and keep looping for next resource.
                    logging.error("Error getting isourl = %s",
                                  isourl,
                                  exc_info=True)
                    errors.append(isourl)
                    continue
                else:
                    # Success fetching metadata.
                    successes.append(isourl)
                    xmlpath = os.path.join(isodir, resource_name+EXTENSION)
                    try:
                        with open(xmlpath, WRITE_MODE) as iso:
                            iso.write(response.text)
                    except:
                        # Could not write metadata file:
                        # log and keep looping for next resource.
                        logging.error("Error writing xmlpath = %s",
                                      xmlpath,
                                      exc_info=True)
                        errors.append(isourl)
                        continue
                    else:
                        # Success writing metadata file.
                        logging.info("metadata = %s", xmlpath)
    return errors, successes

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
        # Could not remove old crawl results file: log and move on.
        logging.error("Error removing crawl results file", exc_info=True)
        print("Could not remove crawl results file.", file=sys.stderr)
    try:
        if os.path.exists(args.reap_retry_path):
            os.remove(args.reap_retry_path)
    except:
        # Could not remove old reap retry file: log and move on.
        logging.error("Error removing reap retry file", exc_info=True)
        print("Could not remove reap retry file.", file=sys.stderr)
    resources = crawl(args.catalog)
    logging.info("Crawl complete for catalog = %s", args.catalog)
    if resources:
        # Did the crawler find any matching resources?
        # Write the crawler results to a file.
        results = ["name = {0}\nurl = {1}\n".format(*resource)
                   for resource in resources]
        format_spec = "Crawler found {0:d} matching resources"
        print(format_spec.format(len(results)), file=sys.stderr)
        try:
            with open(args.crawl_results_path, WRITE_MODE) as crawl_results:
                crawl_results.writelines(results)
        except:
            # Could not write crawl results file: log and move on.
            logging.error("Error writing crawl results file", exc_info=True)
            print("Could not write crawl results file.", file=sys.stderr)
        errors, successes = reap(args.isopath, resources)
        if errors:
            # Did reap fail to write all the metadata?
            # Write the reap retry file.
            format_spec = "Reap failed for {0:d} matching resources"
            print(format_spec.format(len(errors)), file=sys.stderr)
            errors = [error + "\n" for error in errors]
            try:
                with open(args.reap_retry_path, WRITE_MODE) as reap_retry:
                    reap_retry.writelines(errors)
            except:
                # Count not write reap retry file: log and move on.
                logging.error("Error writing reap retry file", exc_info=True)
        if successes:
            # Did reap write any metadata?
            # Write the reap success files.
            format_spec = "Reap succeeded for {0:d} matching resources"
            print(format_spec.format(len(successes)), file=sys.stderr)
            successes = [success + "\n" for success in successes]
            try:
                with open(args.reap_success_path, WRITE_MODE) as reap_success:
                    reap_success.writelines(successes)
            except:
                # Count not write reap success files: log and move on.
                logging.error("Error writing reap success file", exc_info=True)
    logging.info("Harvest complete.")

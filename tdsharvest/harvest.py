import os
import argparse
import requests
import time
import logging
from thredds_crawler.crawl import Crawl

def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog",
                        help="The URL of the catalog.xml to harvest.")
    parser.add_argument("isopath",
                        help="The path in which to place the ISO metadata.")
    parser.add_argument("logpath",
                        help="The path of the logfile.")
    return parser.parse_args()

def reap(catalog):
    c = Crawl(catalog, select=[".*\.nc$", ".*\.ncml$"])
    indices = [(dataset.id, dataset.services) for dataset in c.datasets]
    return [(id_, service["url"]) for id_, services in indices
                                  for service in services
                                  if service["service"]=="ISO"]

def sow(isopath, resource_list):
    os.makedirs(isopath, mode=0o755, exist_ok=True)
    for resource, isourl in resource_list:
        logging.info("resource = %s", resource)
        logging.info("url = %s", isourl)
        resource_dir, resource_name = os.path.split(resource)
        isodir = os.path.join(isopath, resource_dir)
        os.makedirs(isodir, mode=0o755, exist_ok=True)
        response = requests.get(isourl)
        time.sleep(1)
        xmlpath = os.path.join(isodir, resource_name+".xml")
        with open(xmlpath, "w") as iso:
            iso.write(response.text)
        logging.info("metadata = %s", xmlpath)
        print(".", end="")
    return

if __name__ == "__main__":
   args = parseargs()
   logging.basicConfig(filename=args.logpath,
                       level=logging.DEBUG,
                       format="%(levelname)s: %(asctime)s: pid=%(process)d: %(message)s")
   logging.info("Beginning harvest.")
   sow(args.isopath, reap(args.catalog))
   logging.info("Harvest complete.")

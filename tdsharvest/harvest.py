import argparse
from pprint import pprint
from thredds_crawler.crawl import Crawl

def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog",
                        help="The URL of the catalog.xml to harvest.")
    return parser.parse_args()

def harvest(catalog):
    c = Crawl(catalog, select=[".*\.nc$", ".*\.ncml$"])
    indices = [(dataset.id, dataset.services) for dataset in c.datasets]
    return [(id_, service["url"]) for id_, services in indices
                                  for service in services
                                  if service["service"]=="ISO"]

if __name__ == "__main__":
   args = parseargs()
   punchlist = harvest(args.catalog)
   pprint(punchlist)

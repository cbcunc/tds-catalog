
# coding: utf-8

# In[7]:

import os
import re
import requests
import urlparse as up
import netCDF4 as nc
from lxml import etree as et
from StringIO import StringIO


# In[10]:

catalog_pattern = re.compile(r"^catalog\.html\?dataset=(.+)\.(nc|ncml)$")
catalog_url = "http://opendap.renci.org:1935/thredds/catalog/SSV-Ncml/catalog.html"
catalog_components = up.urlparse(catalog_url)
catalog_root = up.urlunsplit((catalog_components[0], catalog_components[1], "", "", ""))
catalog_path = up.urlunsplit((catalog_components[0], catalog_components[1], os.path.split(catalog_components[2])[0], "", ""))
print catalog_root
print catalog_path


# In[94]:

if catalog.ok:
    parser = et.HTMLParser()
    tree = et.parse(StringIO(catalog.text), parser)
    root = tree.getroot()
else:
    catalog.raise_for_status()


# In[87]:

anchors = root.findall(".//a[@href]")
anchors = [anchor for anchor in anchors if catalog_pattern.search(anchor.get("href"))]
landing_urls = [os.path.join(catalog_path, anchor.get("href")) for anchor in anchors]


# In[91]:

landing_pattern = re.compile(r"\\dodsC\\")
opendap_urls = []
for landing_url in landing_urls:
    landing = requests.get(landing_url)
    if landing.ok:
        parser = et.HTMLParser()
        tree = et.parse(StringIO(landing.text), parser)
        root = tree.getroot()
    else:
        landing.raise_for_status()
    anchors = root.findall(".//a[@href]")
    anchors = [anchor for anchor in anchors if landing_pattern.search(anchor.get("href"))]
    opendap_url = os.path.join()


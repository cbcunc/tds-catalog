
# coding: utf-8

# In[29]:

import os
import re
import requests
import urlparse as up
from lxml import etree as et
from StringIO import StringIO


# In[30]:

catalog_pattern = re.compile(r"^catalog\.html\?dataset=(.+)\.(nc|ncml)$")
catalog_url = "http://opendap.renci.org:1935/thredds/catalog/SSV-Ncml/catalog.html"
catalog_url_components = up.urlparse(catalog_url)


# In[31]:

catalog = requests.get(catalog_url)
if catalog.ok:
    parser = et.HTMLParser(recover=True)
    tree = et.parse(StringIO(catalog.content), parser)
    root = tree.getroot()
else:
    catalog.raise_for_status()


# In[32]:

anchors = root.findall(".//a[@href]")
anchors = [anchor for anchor in anchors if catalog_pattern.search(anchor.get("href"))]
catalog_path = os.path.split(catalog_url_components[2])[0]
landing_paths = [os.path.join(catalog_path, anchor.get("href")) for anchor in anchors]
landing_urls = [up.urlunsplit((catalog_url_components[0],
                               catalog_url_components[1],
                               landing_path, "", "")) for landing_path in landing_paths]


# In[33]:

landing_pattern = re.compile("/ncml/")
ncml_urls = []
for landing_url in landing_urls:
    landing = requests.get(landing_url)
    if landing.ok:
        parser = et.HTMLParser()
        tree = et.parse(StringIO(landing.text), parser)
        root = tree.getroot()
    else:
        landing.raise_for_status()
    anchors = root.findall(".//a[@href]")
    ncml_paths = [anchor.text.strip() for anchor in anchors if landing_pattern.search(anchor.get("href"))]
    ncml_urls.extend([up.urlunsplit((catalog_url_components[0],
                                     catalog_url_components[1],
                                     ncml_path, "", "")) for ncml_path in ncml_paths])


# In[34]:

with open("SSV-Ncml.txt", "w") as save:
    save.writelines([url + "\n" for url in ncml_urls])
print ncml_urls


# In[54]:

interesting_attributes = ["cdm_data_type",
                          "conventions",
                          "forecaststarttime",
                          "forecastendtime",
                          "institution",
                          "model",
                          "wind_source",
                          "advisory_or_cycle",
                          "grid",
                          "stormname",
                          "stormtype",
                          "stormyear",
                          "id",
                          "title", ]
ncml_mappings = []
for ncml_url in ncml_urls:
    ncml_attributes = {"location": ncml_url}
    for attribute in interesting_attributes:
        ncml_attributes[attribute] = None
    ncml = requests.get(ncml_url)
    if ncml.ok and ncml.content:
        parser = et.HTMLParser(recover=True)
        tree = et.parse(StringIO(ncml.content), parser)
        root = tree.getroot()
        netcdf = root.find(".//netcdf[@location]")
        attributes = netcdf.findall("attribute[@name]")
        for attribute in attributes:
            name = attribute.attrib["name"].lower()
            if name in interesting_attributes:
                value = attribute.attrib.get("value")
                if value:
                    ncml_attributes[name] = value
    else:
        ncml.raise_for_status()
    ncml_mappings.append(ncml_attributes)


# In[55]:

print ncml_mappings


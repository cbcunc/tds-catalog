
# coding: utf-8

# In[14]:

import os
import sqlite3


# In[15]:

db_name = "SSV-Ncml.db"
db = sqlite3.connect(db_name)
db.close()
try:
    os.remove(db_name)
except OSError:
    print e
db = sqlite3.connect(db_name)
c = db.cursor()


# In[16]:

c.execute("""CREATE TABLE global
             (location TEXT PRIMARY KEY UNIQUE ON CONFLICT FAIL NOT NULL ON CONFLICT FAIL,
              cdm_data_type TEXT,
              conventions TEXT,
              forecaststarttime TEXT,
              forecastendtime TEXT,
              institution TEXT,
              model TEXT,
              wind_source TEXT,
              advisory_or_cycle INT,
              grid TEXT,
              stormname TEXT,
              stormtype TEXT,
              stormyear INT,
              id TEXT,
              title TEXT)""")
db.commit()


# In[17]:

db.close()


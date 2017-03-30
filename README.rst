tdsharvest
==========

A package to harvest metadata from the NetCDF resources in a TDS catalog.

::

    usage: harvest.py [-h]
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

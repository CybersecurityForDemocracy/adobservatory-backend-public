The Ad Screener Backend uses the following files to work properly:

* populate_es.py - This file populates the Elasticsearch cluster with new data from the Ads database and should be run once per day.
* initialize_es.py - This file initializes the templates for a new ES cluster. This file only needs to be run once.


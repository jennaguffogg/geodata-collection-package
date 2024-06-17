# geodata-collection-package
a python package for sourcing some remote sensing and satellite data sources, inspired by USYD's geodata-harvester.

This is an exercise in learning Object-oriented programming and will be updated intermittently.

The original geodata-hervester uses yaml files to configure the data collection, and uses an input point geometry. This package will use json input files (for now) and will use a bounding box as input. It includes some utility functions to also apply masking to the geotiffs and to save the data as a cloud-optimised geotiff (COG).

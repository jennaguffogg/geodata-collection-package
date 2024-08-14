from setuptools import find_packages, setup

VERSION = "0.0.1"

setup(
    name="sensand_gis_utils",
    version=VERSION,
    packages=find_packages(),
    install_requires=[
        "pystac_client==0.7.7",
        "odc-stac",
        "rasterio==1.3.10",
        "rioxarray==0.15.5",
        "rio-cogeo==5.3.0",
        "xarray==2024.5.0",
        "geopandas==0.14.4",
        "pandas==2.2.2",
        "numpy==1.26.4",
        "matplotlib==3.8.4",
        "owslib==0.27.2",
        "retry-requests",
        "openmeteo-requests",
        "requests-cache",
        "requests==2.31.0",
        "pyproj==3.6.1",
    ],
    keywords=["gis", "utils", "stac", "meteo", "colormap"],
    package_data={"data": ["*.json"]},
)

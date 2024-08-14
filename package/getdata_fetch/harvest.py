"""
This script contains the `DataHarvester` class and the `run` method.

The `DataHarvester` class is responsible for fetching geospatial data based on the provided configuration file and input geometry. It has the following attributes:
- `settings`: Stores the loaded settings from the configuration file.
- `input_geom`: Stores the input geometry that defines the area of interest.
- `target_sources`: Stores the target sources specified in the settings.
- `target_bbox`: Stores the target bounding box specified in the settings.
- `property_name`: Stores the property name specified in the settings.
- `output_data_dir`: Stores the output data directory specified in the settings.
- `target_crs`: Stores the target CRS specified in the settings.
- `resample`: Stores the resample method specified in the settings.
- `add_buffer`: Stores the add_buffer flag specified in the settings.
- `data_mask`: Stores the data mask flag specified in the settings.
- `fetched_files`: Stores the list of fetched files.

The `run` method is responsible for executing the data fetching process. It performs the following steps:
1. Adds a buffer to the input geometry if the add_buffer flag is True.
2. Sets the coordinates based on the latitude and longitude specified in the settings.
3. Counts the number of sources to download from and lists the source names.
4. Fetches data from the SLGA, DEM, and Radiometric sources based on the target sources specified in the settings.
5. Applies masking to the downloaded files if the data mask flag is True.

To use this script, create an instance of the `DataHarvester` class and call the `run` method with the path to the configuration file and the input geometry as parameters.
"""

import logging
import os

# from geodata_fetch import getdata_radiometric  # getdata_dem
from geodata_fetch.getdata_dem import (  # updated call to dem using class
    dem_harvest,
    dem_harvest_global,
)
from geodata_fetch.getdata_slga import identifier2depthbounds, slga_harvest
from geodata_fetch.utils import load_settings, reproj_mask

logger = logging.getLogger()
# try this but remove if it doesn't work well with datadog:
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class Settings:
    def __init__(self, config):
        self.target_sources = config.target_sources
        self.target_bbox = config.target_bbox
        self.property_name = config.property_name
        self.outpath = config.outpath
        self.target_crs = config.target_crs
        self.resample = getattr(config, "resample", False)
        self.add_buffer = getattr(config, "add_buffer", False)
        self.data_mask = getattr(config, "data_mask", False)
        self.target_res = getattr(config, "target_res", False)
        self.lat = None
        self.long = None


class data_source_interface:
    def fetch_data(self, settings):
        """Interface method to be overridden by concrete data source handlers."""
        raise NotImplementedError


class data_source_factory:
    @staticmethod
    def get_data_source(source_type):
        if source_type == "DEM":
            print("source type is DEM")
            return DEM_data_source()
        elif source_type == "DEM Global":
            print("source type is DEM Global")
            return glob_DEM_data_source()
        elif source_type == "SLGA":
            print("source type is SLGA")
            return SLGA_data_source()
        else:
            raise ValueError(f"Unknown data source: {source_type}")


class DEM_data_source(data_source_interface):
    def __init__(self):
        self.dem_harvester = dem_harvest()

    def fetch_data(self, settings):
        try:
            dem_data = self.dem_harvester.get_dem_layers(
                property_name=settings.property_name,
                layernames=settings.target_sources["DEM"],
                bbox=settings.target_bbox,
                outpath=settings.outpath,
                crs=settings.target_crs,
            )
            return dem_data
        except Exception as e:
            print(f"Error fetching DEM data: {e}")
            return []


class glob_DEM_data_source(data_source_interface):
    def __init__(self):
        self.dem_harvester_global = dem_harvest_global()

    def fetch_data(self, settings):
        try:
            glob_dem_data = self.dem_harvester_global.get_global_stac_dem(
                property_name=settings.property_name,
                layernames=settings.target_sources["DEM Global"],
                bbox=settings.target_bbox,
                outpath=settings.outpath,
            )
            return glob_dem_data
        except Exception as e:
            print(f"Error fetching DEM Global data: {e}")
            return []


class SLGA_data_source(data_source_interface):
    def __init__(self):
        self.slga_harvester = slga_harvest()

    def fetch_data(self, settings):
        try:
            depth_min = []
            depth_max = []
            for layername in settings.target_sources["SLGA"].keys():
                depth_bounds = settings.target_sources["SLGA"][layername]
                dmin, dmax = identifier2depthbounds(depth_bounds)
                depth_min.append(dmin)
                depth_max.append(dmax)

            files_slga = self.slga_harvester.get_slga_layers(
                property_name=settings.property_name,
                layernames=list(settings.target_sources["SLGA"].keys()),
                bbox=settings.target_bbox,
                outpath=settings.outpath,
                depth_min=depth_min,
                depth_max=depth_max,
                get_ci=False,  # Example flag, should be configured via settings if possible
            )
            return files_slga
        except Exception as e:
            print(f"Error fetching SLGA data: {e}")
            return []


class DataHarvester:
    def __init__(self, path_to_config, input_geom):
        config = load_settings(path_to_config)
        self.settings = Settings(config)
        self.input_geom = input_geom

        self.data_sources = {
            key: data_source_factory.get_data_source(key)
            for key in self.settings.target_sources
        }

    def run(self):
        if self.settings.add_buffer:
            self.input_geom = self.input_geom.buffer(0.002, join_style=2, resolution=15)

        for source_name, source in self.data_sources.items():
            try:
                print(f"processing {source_name}:")
                source.fetch_data(self.settings)
            except Exception as e:
                print(f"error fetching {source_name}: {e}")

        if self.settings.data_mask:
            self.mask_data()

    def mask_data(self):
        try:
            tif_files = [
                f
                for f in os.listdir(self.settings.outpath)
                if f.endswith(".tiff")
                and not f.endswith(
                    ("_masked.tiff", "_colored.tiff", "_cog.tiff", "_cog.public.tiff")
                )
            ]
        except Exception as e:
            logger.error(f"Error listing tiff files: {e}")

        for tif in tif_files:
            try:
                print(f"Masking {tif}")
                reproj_mask(
                    filename=tif,
                    input_filepath=self.settings.outpath,
                    bbox=self.input_geom,
                    out_crscode=self.settings.target_crs,
                    output_filepath=self.settings.outpath,
                    resample=self.settings.resample,
                )
            except Exception as e:
                logger.error(f"Error masking {tif}: {e}")

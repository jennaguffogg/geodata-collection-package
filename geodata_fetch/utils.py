#!/bin/python
import json
import logging
import os
from types import SimpleNamespace

import numpy as np
import rasterio
import rioxarray as rxr
from matplotlib import cm
from matplotlib.colors import Normalize
from owslib.wcs import WebCoverageService
from rasterio.dtypes import uint8
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.plot import reshape_as_raster
from rasterio.warp import Resampling, calculate_default_transform
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logger = logging.getLogger()

## ------ Setup rasterio profiles ------ ##


class Profile:
    """Base class for Rasterio dataset profiles.

    Subclasses will declare a format driver and driver-specific
    creation options.
    """

    driver = None
    defaults = {}

    def __call__(self, **kwargs):
        """Returns a mapping of keyword args for writing a new datasets.

        Example:

            profile = SomeProfile()
            with rasterio.open('foo.tiff', 'w', **profile()) as dst:
                # Write data ...

        """
        if kwargs.get("driver", self.driver) != self.driver:
            raise ValueError("Overriding this profile's driver is not allowed.")
        profile = self.defaults.copy()
        profile.update(**kwargs)
        profile["driver"] = self.driver
        return profile


class DefaultGTiffProfile(Profile):
    """A tiled, band-interleaved, LZW-compressed, 8-bit GTiff profile."""

    driver = "GTiff"
    defaults = {
        "interleave": "band",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "compress": "lzw",
        "nodata": 0,
        "dtype": uint8,
    }


default_gtiff_profile = DefaultGTiffProfile()


def list_tif_files(path):
    try:
        return [f for f in os.listdir(path) if f.endswith(".tiff")]
    except Exception as e:
        logger.error(
            f"Error listing TIFF files in directory {path}: {e}", exc_info=True
        )
        return []


def load_settings(input_settings):
    try:
        if isinstance(input_settings, str):
            with open(input_settings, "r") as f:
                settings = json.load(f)
        else:
            settings = json.load(input_settings)

        settings = SimpleNamespace(**settings)

        settings.date_min = str(settings.date_start)
        settings.date_max = str(settings.date_end)
        return settings
    except json.JSONDecodeError as e:
        logger.error(
            f"JSON decode error in load_settings with input {input_settings}: {e}",
            exc_info=True,
        )
    except FileNotFoundError as e:
        logger.error(
            f"File not found in load_settings: {input_settings}: {e}", exc_info=True
        )
    except Exception as e:
        logger.error(f"Error loading the data harvester settings: {e}", exc_info=True)


def calc_arc2meter(arcsec, latitude):
    """
    Calculate arc seconds to meter

    Input
    -----
    arcsec: float, arcsec
    latitude: float, latitude

    Return
    ------
    (meters Long, meters Lat)
    """
    try:
        meter_lng = arcsec * np.cos(latitude * np.pi / 180) * 30.922
        meter_lat = arcsec * 30.87
        return (meter_lng, meter_lat)
    except Exception as e:
        logger.error(
            f"Error converting arc seconds to meters for arcsec={arcsec}, latitude={latitude}: {e}",
            exc_info=True,
        )
        return None, None


def calc_meter2arc(meter, latitude):
    """
    Calculate meter to arc seconds

    Input
    -----
    meter: float, meter
    latitude: float, latitude

    Return
    ------
    (arcsec Long, arcsec Lat)
    """
    try:
        arcsec_lng = meter / np.cos(latitude * np.pi / 180) / 30.922
        arcsec_lat = meter / 30.87
        return (arcsec_lng, arcsec_lat)
    except Exception as e:
        logger.error(
            f"Error converting meters to arc seconds for meter={meter}, latitude={latitude}: {e}",
            exc_info=True,
        )
        return None, None


def get_wcs_capabilities(url):
    """
    Get capabilities from WCS layer

    NOTE: the url in this case is the 'layers_url' from the json config file.
    The SLGA module is different because there are multiple urls, but for DEM and radiometrics, use the single wcs url provided.
    TODO: Move this back into the individual data modules so it's easier to debug.

    Parameters
    ----------
    url : str
        The URL of the WCS layer.

    Returns
    -------
    keys : list
        A list of layer identifiers.
    titles : list of str
        A list of layer titles.
    descriptions : list of str
        A list of layer descriptions.
    bboxs : list of floats
        A list of layer bounding boxes.
    """
    try:
        # Create WCS object
        wcs = WebCoverageService(url, version="1.0.0", timeout=600)
        content = wcs.contents
        keys = content.keys()

        # Get bounding boxes and crs for each coverage
        print("Following data layers are available:")
        bbox_list = []
        title_list = []
        description_list = []
        for key in keys:
            print(f"key: {key}")
            print(f"title: {wcs[key].title}")
            title_list.append(wcs[key].title)
            print(f"{wcs[key].abstract}")
            description_list.append(wcs[key].abstract)
            print(f"bounding box: {wcs[key].boundingboxes}")
            bbox_list.append(wcs[key].boundingboxes)

        return keys, title_list, description_list, bbox_list
    except Exception as e:
        logger.error(
            f"Error getting WCS capabilities from URL {url}: {e}", exc_info=True
        )
        return None, None, None, None


def reproj_mask(
    filename, input_filepath, bbox, crscode, output_filepath, resample=False
):
    """
    Reprojects and converts a raster file from uint16 to float32, then masks it based on the given parameters,
    ensuring that NoData values are handled correctly throughout the process.

    Args:
        filename (str): The name of the input raster file.
        input_filepath (str): The path to the directory containing the input raster file.
        bbox (geopandas.GeoDataFrame): The bounding box geometry used for clipping the raster.
        crscode (str): The CRS code to reproject the raster to.
        output_filepath (str): The path to the directory where the masked raster will be saved.
        resample (bool, optional): Flag indicating whether to perform pixel resampling. Defaults to False.

    Returns:
        xarray.DataArray: The clipped and reprojected raster as a DataArray.

    """
    input_full_filepath = os.path.join(input_filepath, filename)
    masked_filepath = filename.replace(".tiff", "_masked.tiff")
    mask_outpath = os.path.join(output_filepath, masked_filepath)

    try:
        input_raster = rxr.open_rasterio(input_full_filepath)

        # Convert raster to float32 and handle NoData correctly
        if input_raster.dtype == "uint16":
            # Assume the original NoData value is known, set it as such or detect it
            original_nodata = input_raster.rio.nodata
            input_raster = input_raster.astype("float32")
            # Replace original NoData with NaN in float32
            if original_nodata is not None:
                input_raster = input_raster.where(
                    input_raster != original_nodata, np.nan
                )

        # Update NoData value for float32 in the metadata
        input_raster.rio.write_nodata(np.nan, inplace=True)

        # Run pixel resampling if the flag is set to true
        if resample:
            upscale_factor = 3
            new_width = input_raster.rio.width * upscale_factor
            new_height = input_raster.rio.height * upscale_factor
            input_raster = input_raster.rio.reproject(
                input_raster.rio.crs,
                shape=(int(new_height), int(new_width)),
                resampling=Resampling.nearest,
            )

        # Clip the raster using the geometry, ensuring to invert the mask
        clipped = input_raster.rio.clip(bbox.geometry, crs=input_raster.rio.crs)

        # Reproject the clipped raster and save
        reprojected = clipped.rio.reproject(crscode)
        reprojected.rio.to_raster(mask_outpath, tiled=True, dtype="float32")

        return reprojected
    except Exception as e:
        logger.error(
            f"Error occurred while reprojecting and masking raster {filename}: {e}",
            exc_info=True,
        )
        return None

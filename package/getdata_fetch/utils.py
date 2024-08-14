#!/bin/python
"""
--Function List--
list_tif_files: List all tif files in a directory.

load_settings: Load settings from a JSON file or a file-like object.

arc2meter: Converter arc seconds to meter and vice versa.

meter2arc: Converter arc seconds to meter and vice versa.

get_wcs_capabilities: Get capabilities from WCS layer. Can return some metadata about the dataset as well as individual layers contained in the wcs server.

_getFeatures (internal): Extracts rasterio compatible test from geodataframe.

_read_file: Reads a raster file using rasterio library.

reproj_mask: Masks a raster to the area of a shape, and reprojects.

colour_geotiff_and_save_cog: Colorizes a GeoTIFF image using a specified color map and saves it as a COG (Cloud-Optimized GeoTIFF).

retry_decorator: A decorator to retry the WCS endpoint if an HTTP 502 or 503 error occurs.

"""

# TODO: add function that can take a list or dictionary of variables and create the json-like object needed by load_settings. This removes it from the notebooks and user's responsibility.

import json
import logging
import os
import time
from functools import wraps
from types import SimpleNamespace

import numpy as np
import rasterio
import rioxarray as rxr
from matplotlib import cm
from matplotlib.colors import Normalize
from owslib.wcs import WebCoverageService
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.plot import reshape_as_raster
from rasterio.warp import Resampling, calculate_default_transform
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logger = logging.getLogger()
# try this but remove if it doesn't work well with datadog:
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def list_tif_files(path):
    try:
        return [f for f in os.listdir(path) if f.endswith(".tiff")]
    except Exception as e:
        logger.error(
            f"Error listing TIFF files in directory {path}: {e}", exc_info=True
        )
        return []


def load_settings(input_settings):
    """
    Load settings from a JSON file or a file-like object.

    Args:
        input_settings (str or file-like object): The input settings. It can be either a string representing the path to a JSON file or a file-like object containing the JSON data.

    Returns:
        settings (namespace): The loaded settings as a namespace object. The settings can be accessed using dot notation, e.g., `settings.variable_name`.

    Raises:
        ValueError: If the input_settings is neither a string nor a file-like object.

    """
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


def _getFeatures(gdf):
    """
    Internal function to parse features from GeoDataFrame in such a manner that
    rasterio wants them.

    INPUTS
        gdf: geodataframe

    RETURNS
        json object for rasterio to read
    """
    try:
        return [json.loads(gdf.to_json())["features"][0]["geometry"]]
    except Exception as e:
        logger.error(f"Error extracting features from GeoDataFrame: {e}", exc_info=True)
        return None


def _read_file(file):
    """
    Reads a raster file using rasterio library.

    Args:
        file (str): The path to the raster file.

    Returns:
        numpy.ndarray: The raster data as a NumPy array.

    Raises:
        rasterio.errors.RasterioIOError: If the file cannot be opened or read.

    """
    try:
        with rasterio.open(file) as src:
            temp = src.read()
            dims = temp.shape[0]
            if dims == 1:
                return src.read(1)
            else:
                return src.read()
    except rasterio.errors.RasterioIOError as e:
        logger.error(f"Rasterio IO error reading file {file}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error reading file {file}: {e}", exc_info=True)
        return None


def reproj_mask(
    filename, input_filepath, bbox, out_crscode, output_filepath, resample=False
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

    """
    TODO: do a reproject on the incoming geometry so it's always 3857
    TODO: Do a reproject check on the incoming raster so its always 3857
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

        if input_raster.rio.crs.to_epsg() != out_crscode:
            logger.info(
                f"Reprojecting raster, input crs:{input_raster.rio.crs}, output crs:{out_crscode}"
            )
            input_raster = input_raster.rio.reproject(out_crscode)

        if bbox.crs != out_crscode:
            logger.info(
                f"Reprojecting geometry, input crs:{bbox.crs}, output crs:{out_crscode}"
            )
            bbox = bbox.to_crs(out_crscode)

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
        clipped = input_raster.rio.clip(
            bbox.geometry, crs=input_raster.rio.crs, all_touched=True
        )

        # Reproject the clipped raster and save
        reprojected = clipped.rio.reproject(out_crscode)
        reprojected.rio.to_raster(mask_outpath, tiled=True, dtype="float32")

        return reprojected
    except Exception as e:
        logger.error(
            f"Error occurred while reprojecting and masking raster {filename}: {e}",
            exc_info=True,
        )
        return None


def colour_geotiff_and_save_cog(input_geotiff, colour_map):
    output_colored_tiff_filename = input_geotiff.replace(".tiff", "_colored.tiff")
    output_cog_filename = input_geotiff.replace(".tiff", "_cog.public.tiff")

    try:
        with rasterio.open(input_geotiff) as src:
            meta = src.meta.copy()
            dst_crs = rasterio.crs.CRS.from_epsg(4326)
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds
            )

            meta.update(
                {
                    "crs": dst_crs,
                    "transform": transform,
                    "width": width,
                    "height": height,
                }
            )

            tif_data = src.read(1, masked=True).astype("float32")
            tif_formatted = tif_data.filled(np.nan)

            cmap = cm.get_cmap(colour_map)
            na = tif_formatted[~np.isnan(tif_formatted)]

            min_value = min(na)
            max_value = max(na)

            norm = Normalize(vmin=min_value, vmax=max_value)

            coloured_data = (cmap(norm(tif_formatted))[:, :, :3] * 255).astype(np.uint8)

            meta.update({"count": 3})

            with rasterio.open(output_colored_tiff_filename, "w", **meta) as dst:
                reshape = reshape_as_raster(coloured_data)
                dst.write(reshape)

        try:
            dst_profile = cog_profiles.get("deflate")
            with MemoryFile() as mem_dst:
                cog_translate(
                    output_colored_tiff_filename,
                    output_cog_filename,
                    config=dst_profile,
                    in_memory=True,
                    dtype="uint8",
                    add_mask=False,
                    nodata=0,
                    dst_kwargs=dst_profile,
                )

        except Exception as e:
            logger.error(
                f"Error converting {output_colored_tiff_filename} to COG: {e}",
                exc_info=True,
            )
            raise
    except Exception as e:
        logger.error(f"Error colorizing GeoTIFF {input_geotiff}: {e}", exc_info=True)


def retry_decorator(max_retries=3, backoff_factor=1, retry_statuses=(502, 503)):
    """
    A decorator to retry a function if it raises specified HTTP errors.

    Args:
        max_retries (int): The maximum number of retries.
        backoff_factor (float): The factor by which the wait time increases.
        retry_statuses (tuple): HTTP status codes that trigger a retry.

    Returns:
        function: The wrapped function with retry logic.
    """

    def decorator_retry(func):
        @wraps(func)
        # having the func wrapper andt args, kwargs gives access to the function itself and its arguments.
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if e.response.status_code in retry_statuses:
                        print(e.response.status_code)
                        attempts += 1
                        sleep_time = backoff_factor * (2**attempts)
                        time.sleep(sleep_time)
                        logger.error(
                            f"HTTP error {e.response.status_code} occurred. Retrying."
                        )
                    else:
                        raise

            logger.error("Max retries exceeded. Giving up.")

        return wrapper

    return decorator_retry

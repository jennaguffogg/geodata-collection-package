import json
import logging
import os
from datetime import datetime, timezone
from importlib import resources

from owslib.wcs import WebCoverageService

from geodata_fetch.utils import retry_decorator

logger = logging.getLogger()


def get_radiometricdict():
    try:
        with resources.open_text("data", "radiometric_default_config.json") as f:
            rm_json = json.load(f)

        rmdict = {}
        rmdict["title"] = rm_json["title"]
        rmdict["description"] = rm_json["description"]
        rmdict["license"] = rm_json["license"]
        rmdict["source_url"] = rm_json["source_url"]
        rmdict["copyright"] = rm_json["copyright"]
        rmdict["attribution"] = rm_json["attribution"]
        rmdict["crs"] = rm_json["crs"]
        rmdict["resolution_arcsec"] = rm_json["resolution_arcsec"]
        rmdict["layers_url"] = rm_json["layers_url"]
        rmdict["layer_names"] = rm_json["layer_names"]

        return rmdict
    except Exception as e:
        logger.error("Error loading radiometric.json", extra=dict({"error": str(e)}))
        return None


"""
TODO: resolution is set to 1 here. But its 3.6 (100m) in the original data. Remove or find out why its set to 1.
Ditto EPSG.
"""


def get_radiometric_layers(property_name, layernames, bbox, outpath):
    """
    Wrapper function for downloading radiometric data layers and save geotiffs from WCS layer.

    Parameters
    ----------
    outpath: str
        output path
    layername : list of strings
        layer identifiers
    bbox : list
        layer bounding box

    These are now being read from the dict, not passed as ahrd-coded values:
    resolution, url, crs

    Return
    ------
    list of output filenames
    """
    rm_data = get_radiometricdict()

    resolution = rm_data["resolution_arcsec"]
    crs = rm_data["crs"]
    url = rm_data["layers_url"]

    # for url to be called the getdict function needs to be called first
    # url = "https://gsky.nci.org.au/ows/national_geophysical_compilations?service=WCS"

    if type(layernames) != list:
        layernames = [layernames]

    # Loop over all layers
    fnames_out = []
    for layername in layernames:
        outfname = os.path.join(
            outpath, "radiometric_" + layername + "_" + property_name + ".tiff"
        )
        ok = get_radiometric_image(
            outfname=outfname,
            layername=layername,
            bbox=bbox,
            url=url,
            resolution=resolution,
            crs=crs,
        )
        if ok:
            fnames_out.append(outfname)
    return fnames_out


@retry_decorator()
def get_radiometric_image(outfname, layername, bbox, url, resolution, crs):
    """
    Download radiometric data layer and save geotiff from WCS layer.

    Parameters
    ----------
    outfname : str
        output file name
    layername : str
        layer identifier
    bbox : list
        layer bounding box
    resolution : int
    url : str
    crs: str

    Return
    ------
    Exited ok: boolean
    """
    # If the resolution passed is None, set to native resolution of datasource
    if resolution is None:
        resolution = get_radiometricdict()["resolution_arcsec"]

    # Convert resolution into width and height pixel number
    width = abs(bbox[2] - bbox[0])
    height = abs(bbox[3] - bbox[1])
    nwidth = int(width / resolution * 3600)
    nheight = int(height / resolution * 3600)

    # Get date
    times = get_times(url, layername)
    # There is only one time available per layer
    date = times[0]
    # Get data
    if os.path.exists(outfname):
        logger.info(f"{layername}.tiff already exists, skipping download")
    else:
        try:
            wcs = WebCoverageService(url, version="1.0.0", timeout=300)
            data = wcs.getCoverage(
                identifier=layername,
                time=[date],
                bbox=bbox,
                format="GeoTIFF",
                crs=crs,
                width=nwidth,
                height=nheight,
            )
        except Exception as e:
            logger.error(f"Error fetching RadMap wcs: {e}")
            return False

        # Save data
        with open(outfname, "wb") as f:
            f.write(data.read())
        logger.info(f"Layer {layername} saved in {outfname}")

    return True


def get_times(url, layername, year=None):
    """
    Return available dates for layer.

    Parameters
    ----------
    url: str, layer url
    layername: str, name of layer id
    year: int or str, year of interest (if None, times for all available years are returned)

    Return
    ------
    list of dates
    """

    wcs = WebCoverageService(url, version="1.0.0", timeout=300)
    times = wcs[layername].timepositions
    if year is None:
        return times
    else:
        year = int(year)
        dates = []
        for time in times:
            if datetime.fromisoformat(time[:-1]).astimezone(timezone.utc).year == year:
                dates.append(time)
        return dates

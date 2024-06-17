import json
import logging
import os
from importlib import resources

from owslib.coverage.wcsBase import ServiceException
from owslib.wcs import WebCoverageService
from requests.exceptions import HTTPError

logger = logging.getLogger()
# try this but remove if it doesn't work well with datadog:
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def get_slgadict():
    try:
        with resources.open_text("data", "slga_soil.json") as f:
            slga_json = json.load(f)

        slgadict = {}
        slgadict["title"] = slga_json["title"]
        slgadict["description"] = slga_json["description"]
        slgadict["license"] = slga_json["license"]
        slgadict["source_url"] = slga_json["source_url"]
        slgadict["copyright"] = slga_json["copyright"]
        slgadict["attribution"] = slga_json["attribution"]
        slgadict["crs"] = slga_json["crs"]
        slgadict["resolution_arcsec"] = slga_json["resolution_arcsec"]
        slgadict["depth_min"] = slga_json["depth_min"]
        slgadict["depth_max"] = slga_json["depth_max"]
        slgadict["layers_url"] = slga_json["layers_url"]

        return slgadict
    except Exception as e:
        logger.error(
            "Error loading slga_soil.json to getdata_slga module.", exec_info=True
        )
        raise ValueError(
            f"Error loading slga_soil.json to getdata_slga module: {e}"
        ) from e


def get_wcsmap(url, identifier, crs, bbox, resolution, outfname):
    """
    Download and save geotiff from WCS layer

    Parameters
    ----------
    url : str
        the SLGA attribute endpoint e.g. soil organic carbon, available water capacity
    identifier : str
        layer depth identifier e.g 0-5cm, 5-15cm
    crs : str
        layer crs
    bbox : list
        layer bounding box
    resolution : int?
        layer resolution
    outfname : str
        output file name

    """
    if resolution is None:
        resolution = get_slgadict()["resolution_arcsec"]

    # Create WCS object
    filename = os.path.basename(outfname)
    try:
        # for the given endpoint e.g. Organic_Carbon, connect to the web coverage service
        wcs = WebCoverageService(
            url, version="1.0.0", timeout=600
        )  # upping the timeout to see if this reduces SLGA failures

        # Use the WCS to download the data as geotiffs. Here, identifier refers to the soil depth e.g. 0-5cm, 5-15cm depth.
        data = wcs.getCoverage(
            identifier,
            format="GEOTIFF",
            bbox=bbox,
            crs=crs,
            resx=resolution,
            resy=resolution,
        )

        # Save data
        with open(outfname, "wb") as f:
            f.write(data.read())
            logger.info(f"WCS data downloaded and saved as {filename}")
        return True  # where is this being invoked? Can i terminate the whole harvest if this returns false?
    except ServiceException as e:
        logger.error(
            f"WCS server returned exception while trying to download {filename}: {e} ",
            exec_info=True,
        )
        # raise
        return False
    except HTTPError as e:
        # Check the status code of the HTTPError
        if e.response.status_code == 502:
            logger.error(
                f"HTTPError 502: Bad Gateway encountered when accessing {url}",
                exec_info=True,
            )
        elif e.response.status_code == 503:
            logger.error(
                f"HTTPError 503: Service Unavailable encountered when accessing {url}",
                exec_info=True,
            )
        else:
            logger.error(
                f"HTTPError {e.response.status_code}: {e.response.reason} when accessing {url}",
                exec_info=True,
            )
        # raise  # Re-raise the exception after logging
        return False
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}", exec_info=True)
        raise


def depth2identifier(depth_min, depth_max):
    """
    Get identifiers that correspond to depths and their corresponding confidence interval identifiers
    that lie within the depth range depth_min to depth_max.

    Parameters
    ----------
    depth_min : minimum depth [cm]
    depth_max : maximum depth [cm]

    Returns
    -------
    identifiers : layer identifiers
    identifiers_ci_5pc : identifiers for confidence interval 5%
    identifiers_ci_95pc : identifiers for confidence interval 95%
    depth_lower : lower depth of interval
    depth_upper : upper depth of interval
    """
    try:
        depth_intervals = [0, 5, 15, 30, 60, 100, 200]
        identifiers = []
        identifiers_ci_5pc = []
        identifiers_ci_95pc = []
        depths_lower = []
        depths_upper = []
        # Loop over depth intervals
        for i in range(len(depth_intervals) - 1):
            if (depth_min <= depth_intervals[i]) & (
                depth_max >= depth_intervals[i + 1]
            ):
                identifiers.append(str(3 * i + 1))
                identifiers_ci_5pc.append(str(3 * i + 3))
                identifiers_ci_95pc.append(str(3 * i + 2))
                depths_lower.append(depth_intervals[i])
                depths_upper.append(depth_intervals[i + 1])
        return (
            identifiers,
            identifiers_ci_5pc,
            identifiers_ci_95pc,
            depths_lower,
            depths_upper,
        )
    except Exception:
        logger.error(
            "Failed to get identifiers",
            exc_info=True,
            extra={"depth_min": depth_min, "depth_max": depth_max},
        )
        return None, None, None, None, None


def identifier2depthbounds(depths):
    """
    Get min and max depth of list of depth strings

    Parameters
    ----------
    depth_list: list of depth

    Returns
    -------
    min depth
    max depth
    """
    depth_options = ["0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm"]
    depth_intervals = [0, 5, 15, 30, 60, 100, 200]
    try:
        # Check first if entries valid
        for depth in depths:
            assert (
                depth in depth_options
            ), f"depth should be one of the following options {depth_options}"
        # find min and max depth
        ncount = 0
        for i in range(len(depth_options)):
            if depth_options[i] in depths:
                depth_max = depth_intervals[i + 1]
                if ncount == 0:
                    depth_min = depth_intervals[i]
                ncount += 1
        assert ncount == len(depths), f"ncount = {ncount}"
        return depth_min, depth_max
    except Exception:
        logger.error(
            "Failed to get min and max depth",
            exc_info=True,
            extra={"depths": depths},
        )
        return None, None


def get_slga_layers(
    property_name,
    layernames,
    bbox,
    outpath,
    resolution=3,
    depth_min=0,
    depth_max=200,
    get_ci=False,
):
    """
    Download layers from SLGA and saves as geotif.

    Parameters
    ----------
    layernames : list of layer names
    bbox : bounding box [min, miny, maxx, maxy] in
    resolution : resolution in arcsec (Default: 3 arcsec ~ 90m, which is native resolution of SLGA data)
    depth_min : minimum depth (Default: 0 cm). If depth_min and depth_max are lists, then must have same length as layernames
    depth_max : maximum depth (Default: 200 cm, maximum depth of SLGA data)
    outpath : output path

    Returns
    -------
    fnames_out : list of output file names
    """
    try:
        # Check if layernames is a list
        if not isinstance(layernames, list):
            layernames = [layernames]

        # Check if depth_min and depth_max are lists:
        if not isinstance(depth_min, list):
            depth_min = [depth_min] * len(layernames)
        if not isinstance(depth_max, list):
            depth_max = [depth_max] * len(layernames)

        assert len(depth_min) == len(
            depth_max
        ), "depth_min and depth_max should be lists of same length"
        assert len(depth_min) == len(
            layernames
        ), "depth_min and depth_max should be lists with same length as layernames"

        # Check if outpath exist, if not create it
        os.makedirs(outpath, exist_ok=True)

        # If the resolution passed is None, set to native resolution of datasource
        if resolution is None:
            resolution = get_slgadict()["resolution_arcsec"]

        # Get SLGA dictionary
        slgadict = get_slgadict()
        layers_url = slgadict["layers_url"]

        # Convert resolution from arcsec to degree
        resolution_deg = resolution / 3600.0

        # set source crs based on config json
        crs = slgadict["crs"]

        fnames_out = []
        for idx, layername in enumerate(layernames):
            # Get layer url
            layer_url = layers_url[layername]
            # Get depth identifiers for layers
            (
                identifiers,
                identifiers_ci_5pc,
                identifiers_ci_95pc,
                depth_lower,
                depth_upper,
            ) = depth2identifier(depth_min[idx], depth_max[idx])
            for i in range(len(identifiers)):
                identifier = identifiers[i]
                # Get layer name
                layer_depth_name = (
                    f"SLGA_{layername}_{depth_lower[i]}-{depth_upper[i]}cm"
                )
                # Layer fname

                fname_out = os.path.join(
                    outpath, f"{layer_depth_name}_{property_name}.tiff"
                )
                # download data
                dl = get_wcsmap(
                    layer_url, identifier, crs, bbox, resolution_deg, fname_out
                )
                fnames_out.append(fname_out)
            if get_ci:
                for i in range(len(identifiers)):
                    # 5th percentile
                    identifier = identifiers_ci_5pc[i]
                    # Get layer name
                    layer_depth_name = (
                        f"SLGA_{layername}_{depth_lower[i]}-{depth_upper[i]}cm"
                    )
                    # Layer fname
                    fname_out = os.path.join(
                        outpath, f"{layer_depth_name}_{property_name}_5percentile.tiff"
                    )
                    # download data
                    get_wcsmap(
                        layer_url, identifier, crs, bbox, resolution_deg, fname_out
                    )
                    # 95th percentile
                    identifier = identifiers_ci_95pc[i]
                    # Get layer name
                    layer_depth_name = (
                        f"SLGA_{layername}_{depth_lower[i]}-{depth_upper[i]}cm"
                    )
                    # Layer fname
                    fname_out = os.path.join(
                        outpath, f"{layer_depth_name}_{property_name}_95percentile.tiff"
                    )
                    # download data
                    dl = get_wcsmap(
                        layer_url, identifier, crs, bbox, resolution_deg, fname_out
                    )

        return fnames_out
    except Exception:
        logger.error(
            "Failed to get SLGA layers",
            exc_info=True,
            extra={"layernames": layernames},
        )
        return None

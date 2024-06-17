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


class dem_harvest:
    def __init__(self):
        try:
            with resources.open_text("data", "dem.json") as f:
                dem_json = json.load(f)
            self.initialise_attributes_from_json(dem_json)
        except Exception as e:
            logger.error(
                "Error loading dem.json to dem_harvest module.", exec_info=True
            )
            raise ValueError(
                f"Error loading dem.json to dem_harvest module: {e}"
            ) from e

    def initialise_attributes_from_json(self, dem_json):
        self.title = dem_json.get("title")
        self.description = dem_json.get("description")
        self.license = dem_json.get("license")
        self.source_url = dem_json.get("source_url")
        self.copyright = dem_json.get("copyright")
        self.attribution = dem_json.get("attribution")
        self.crs = dem_json.get("crs")
        self.bbox = dem_json.get("bbox")
        self.resolution_arcsec = dem_json.get("resolution_arcsec")
        self.layers_url = dem_json.get("layers_url")
        self.fetched_files = []

    def getwcs_dem(self, url, crs, resolution, bbox, property_name, outpath):
        """
        Downloads a Digital Elevation Model (DEM) using the Web Coverage Service (WCS) protocol.

        Args:
            url (str): The URL of the WCS server.
            crs (str): The coordinate reference system (CRS) of the requested data.
            resolution (float): The resolution of the requested data in arcseconds.
            bbox (tuple): The bounding box of the requested data in the format (minx, miny, maxx, maxy).
            property_name (str): The name of the property associated with the DEM.
            outpath (str): The output directory where the downloaded DEM will be saved.

        Returns:
            str: The filepath of the downloaded DEM.

        Raises:
            ServiceException: If the WCS server returns an exception.
            HTTPError: If there is an HTTP error while accessing the WCS server.
            Exception: If there is a general error while downloading the DEM.

        """
        try:
            if resolution is None:
                resolution = self.resolution_arcsec

            wcs = WebCoverageService(url, version="1.0.0", timeout=600)
            # layername is handled differently here compared to SLGA due to structure of the endpoint
            layername = wcs["1"].title
            fname_out = layername.replace(" ", "_") + "_" + property_name + ".tiff"
            outfname = os.path.join(outpath, fname_out)

            print(outfname)

            os.makedirs(outpath, exist_ok=True)

            data = wcs.getCoverage(
                identifier="1",
                bbox=bbox,
                format="GeoTIFF",
                crs=crs,
                resx=resolution,
                resy=resolution,
            )

            with open(outfname, "wb") as f:
                f.write(data.read())
                logger.info(f"WCS data downloaded and saved as {fname_out}")
        except ServiceException as e:
            logger.error(
                f"WCS server returned exception while trying to download {fname_out}: {e} ",
                exec_info=True,
            )
            return False
        except HTTPError as e:
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
            return False
        except Exception as e:
            logger.error(f"Failed to download {fname_out}: {e}", exec_info=True)
            raise
        return outfname

    def get_dem_layers(self, property_name, layernames, bbox, outpath):
        """
        Fetches DEM layers based on the provided parameters.

        Args:
            property_name (str): The name of the property.
            layernames (str or list): The name(s) of the DEM layer(s) to fetch.
            bbox (tuple): The bounding box coordinates (xmin, ymin, xmax, ymax).
            outpath (str): The output path to save the fetched layers.

        Returns:
            list: A list of file names of the fetched DEM layers.

        Raises:
            Exception: If there is an error while fetching the DEM layers.

        """
        try:
            if not isinstance(layernames, list):
                layernames = [layernames]

            os.makedirs(outpath, exist_ok=True)

            fnames_out = []
            for layername in layernames:
                if layername == "DEM":
                    outfname = self.getwcs_dem(
                        url=self.layers_url["DEM"],
                        crs=self.crs,
                        resolution=self.resolution_arcsec,
                        bbox=bbox,
                        property_name=property_name,
                        outpath=outpath,
                    )
                    if outfname:
                        fnames_out.append(outfname)

            return fnames_out
        except Exception as e:
            logger.error(
                "Failed to get DEM layers",
                exc_info=True,
                extra={"layernames": layernames, "error": str(e)},
            )
            return None


"""
Below is old code before this module was refactored using a class. 
It is kept here for reference and comparison purposes.
"""
# def get_dem_layers(property_name, layernames, bbox, outpath):
#     """
#     Download DEM-H layer and save as a geotif.

#     Parameters
#     ----------
#     layernames : list of layer names (in this case, only 1)
#     bbox : bounding box [min, miny, maxx, maxy] in
#     outpath : output path

#     Returns
#     -------
#     fnames_out : list of output file names
#     """
#     try:
#         if not isinstance(layernames, list):
#             layernames = [layernames]

#         # Check if outpath exist, if not create it
#         os.makedirs(outpath, exist_ok=True)

#         demdict = get_demdict()
#         resolution = demdict["resolution_arcsec"]
#         # Convert resolution from arcsec to degree
#         # resolution_deg = resolution / 3600.0

#         # set target crs based on config json
#         crs = demdict["crs"]
#         layers_url = demdict["layers_url"]
#         dem_url = layers_url["DEM"]

#         fnames_out = []
#         for layername in layernames:
#             if layername == "DEM":
#                 outfname = getwcs_dem(
#                     url=dem_url,
#                     crs=crs,
#                     resolution=resolution,
#                     bbox=bbox,
#                     property_name=property_name,
#                     outpath=outpath,
#                 )
#             fnames_out.append(outfname)

#         return fnames_out
#     except Exception:
#         logger.error(
#             "Failed to get DEM layer",
#             exc_info=True,
#             extra={"layernames": layernames},
#         )
#         return None


# def get_demdict():
#     try:
#         with resources.open_text("data", "dem.json") as f:
#             dem_json = json.load(f)

#         demdict = {}
#         demdict["title"] = dem_json["title"]
#         demdict["description"] = dem_json["description"]
#         demdict["license"] = dem_json["license"]
#         demdict["source_url"] = dem_json["source_url"]
#         demdict["copyright"] = dem_json["copyright"]
#         demdict["attribution"] = dem_json["attribution"]
#         demdict["crs"] = dem_json["crs"]
#         demdict["bbox"] = dem_json["bbox"]
#         demdict["resolution_arcsec"] = dem_json["resolution_arcsec"]
#         demdict["layers_url"] = dem_json["layers_url"]

#         return demdict
#     except Exception as e:
#         logger.error(
#             "Error loading dem.json to getdata_slga module.", exec_info=True
#         )
#         raise ValueError(
#             f"Error loading dem.json to getdata_slga module: {e}"
#         ) from e

# def getwcs_dem(url, crs, resolution, bbox, property_name, outpath):
#     """
#     Download and save geotiff from WCS layer

#     Parameters
#     ----------
#     outpath : str
#         output directory for the downloaded file
#         NOTE: The outpath is used here instead of an outfname because there's only 1 layer and we're naming the tif on its title. For the SLGA and others, there are multiple layers so this is set earlier based on the contents of the config and settings jsons.
#     bbox : list
#         layer bounding box
#     resolution : int
#         layer resolution in arcsecond
#     url : str
#         url of wcs server
#     crs: str
#     outpath: str
#         output directory for the downloaded file

#     Return
#     ------
#     Output filename
#     """

#     if resolution is None:
#         resolution = get_demdict()["resolution_arcsec"]

#     # Create WCS object and get data
#     try:
#         wcs = WebCoverageService(url, version="1.0.0", timeout=600)
#         layername = wcs["1"].title
#         fname_out = layername.replace(" ", "_") + "_" + property_name + ".tiff"
#         outfname = os.path.join(outpath, fname_out)
#         data = wcs.getCoverage(
#             identifier="1",
#             bbox=bbox,
#             format="GeoTIFF",
#             crs=crs,
#             resx=resolution,
#             resy=resolution,
#         )
#         # Save data to file
#         with open(outfname, "wb") as f:
#             f.write(data.read())
#             logger.info(f"WCS data downloaded and saved as {fname_out}")
#     except ServiceException as e:
#         logger.error(
#             f"WCS server returned exception while trying to download {fname_out}: {e} ",
#             exec_info=True,
#         )
#         # raise
#         return False
#     except HTTPError as e:
#         # Check the status code of the HTTPError
#         if e.response.status_code == 502:
#             logger.error(
#                 f"HTTPError 502: Bad Gateway encountered when accessing {url}",
#                 exec_info=True,
#             )
#         elif e.response.status_code == 503:
#             logger.error(
#                 f"HTTPError 503: Service Unavailable encountered when accessing {url}",
#                 exec_info=True,
#             )
#         else:
#             logger.error(
#                 f"HTTPError {e.response.status_code}: {e.response.reason} when accessing {url}",
#                 exec_info=True,
#             )
#         # raise  # Re-raise the exception after logging
#         return False
#     except Exception as e:
#         logger.error(f"Failed to download {fname_out}: {e}", exec_info=True)
#         raise
#     return outfname

import json
import logging
import os
from importlib import resources

import rioxarray
from odc.stac import configure_rio, stac_load
from owslib.wcs import WebCoverageService
from pystac_client import Client
from rasterio.io import MemoryFile

from geodata_fetch.utils import retry_decorator

logger = logging.getLogger()
# try this but remove if it doesn't work well with datadog:
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

configure_rio(cloud_defaults=True, aws={"aws_unsigned": True})
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"


class _BaseHarvest:
    def __init__(self, config_filename):
        try:
            with resources.open_text("data", config_filename) as f:
                config_json = json.load(f)
            self.initialise_attributes_from_json(config_json)
        except Exception as e:
            logger.error(
                f"Error loading {config_filename} to {self.__class__.__name__} module.",
                exec_info=True,
            )
            raise ValueError(
                f"Error loading {config_filename} to {self.__class__.__name__} module: {e}"
            ) from e

    def initialise_attributes_from_json(self, config_json):
        self.title = config_json.get("title")
        self.description = config_json.get("description", None)
        self.license = config_json.get("license", None)
        self.source_url = config_json.get("source_url")
        self.copyright = config_json.get("copyright", None)
        self.attribution = config_json.get("attribution", None)
        self.crs = config_json.get("crs")
        self.bbox = config_json.get("bbox", None)
        self.resolution_arcsec = config_json.get("resolution_arcsec", None)
        self.resolution_metre = config_json.get("resolution_metre", None)
        self.layers_url = config_json.get("layers_url")
        self.fetched_files = []


class dem_harvest(_BaseHarvest):
    def __init__(self):
        super().__init__("australia_dem_default_config.json")

    @retry_decorator()
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
            data array containing pixels

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
            # layername = wcs["1"].title
            # fname_out = layername.replace(" ", "_") + "_" + property_name + ".tiff"
            # outfname = os.path.join(outpath, fname_out)

            os.makedirs(outpath, exist_ok=True)

            data = wcs.getCoverage(
                identifier="1",
                bbox=bbox,
                format="GeoTIFF",
                crs=crs,
                resx=resolution,
                resy=resolution,
            )
        except Exception as e:
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
                    f"Error {e.response.status_code}: {e.response.reason} when accessing {url}",
                    exec_info=True,
                )
        return data.read()  # outfname

    def get_dem_layers(self, property_name, layernames, bbox, crs, outpath):
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

            """
            TODO: Adjust resolution param to take ana rcsecond OR metre as input rather than hard-code here.
            """
            resolution = self.resolution_metre

            fnames_out = []
            for layername in layernames:
                if layername == "DEM":
                    data = self.getwcs_dem(
                        url=self.layers_url["DEM"],
                        crs=self.crs,
                        resolution=resolution,
                        bbox=bbox,
                        property_name=property_name,
                        outpath=outpath,
                    )
                    fname_out = f"DEM_SRTM_1_Second_Hydro_Enforced_{property_name}.tiff"
                    outfname = os.path.join(outpath, fname_out)

                    # take the downlaoded data, project it to correct CRS and save:
                    # Load data into rioxarray, reproject, and save
                    with MemoryFile(data) as memfile:
                        with memfile.open() as src:
                            rxr = rioxarray.open_rasterio(src, masked=True)
                            rxr_reprojected = rxr.rio.reproject("EPSG:3857")
                            rxr_reprojected.rio.to_raster(outfname)
                            fnames_out.append(outfname)
                            logger.info(f"Reprojected WCS data saved as {fname_out}")

            return fnames_out
        except Exception as e:
            logger.error(
                "Failed to get DEM layers",
                exc_info=True,
                extra={"layernames": layernames, "error": str(e)},
            )
            return None


class dem_harvest_global(_BaseHarvest):
    def __init__(self):
        super().__init__("stac_global_dem_default_config.json")

    def get_global_stac_dem(self, property_name, layernames, bbox, outpath):
        if not isinstance(layernames, list):
            layernames = [layernames]
        try:
            os.makedirs(outpath, exist_ok=True)

            fnames_out = []
            for layername in layernames:
                if layername == "DEM Global":
                    fname_out = (
                        layername.replace(" ", "_")
                        + "_COP_30_GLO_"
                        + property_name
                        + ".tiff"
                    )

                    outfname = os.path.join(outpath, fname_out)
                    """
                    There are some oddities with handling CRS here. The stac items need to be passed to stac_load_xarray as a SPATIAL reference system (3857) but our final stored data expects a  CARTESIAN system (4326). So, we need to do a reproject after downloading the data.
                    """
                    resolution = 30
                    collections = ["cop-dem-glo-30"]

                    """
                    not sure if importing gis_utils stac was causing a problem, so I've hardcoded in here for now.
                    """
                    catalog = Client.open(self.source_url)
                    query = catalog.search(collections=collections, bbox=bbox)
                    items = list(query.items())

                    stac_load_xarray = stac_load(
                        items,
                        crs="epsg:3857",
                        resolution=resolution,
                        bbox=bbox,
                        chunksize=(1024, 1024),
                    )  # only squeeze if you KNOW there is only one time dimension
                    # print("checkpoint before memory load of dem")
                    stac_load_xarray = stac_load_xarray.squeeze()
                    stac_load_xarray = stac_load_xarray.load()
                    xarray_data = stac_load_xarray.data

                    final_raster = xarray_data.rio.to_raster(outfname, driver="COG")
                    fnames_out.append(final_raster)
            return fnames_out
        except Exception as e:
            logger.error(
                "Failed to get Global DEM layer",
                exc_info=True,
                extra={"layernames": layernames, "error": str(e)},
            )
            return None

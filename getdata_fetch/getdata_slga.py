import json
import logging
import os
from importlib import resources

from owslib.wcs import WebCoverageService

from geodata_fetch.utils import retry_decorator

logger = logging.getLogger()
# try this but remove if it doesn't work well with datadog:
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class slga_harvest:
    def __init__(self):
        self.load_configuration()

    def load_configuration(self):
        try:
            with resources.open_text("data", "slga_soil_default_config.json") as f:
                config_json = json.load(f)
            self.initialise_attributes_from_json(config_json)
        except Exception as e:
            logger.error(f"Error loading slga_soil.json to dem_harvest module: {e}")

    def initialise_attributes_from_json(self, slga_json):
        self.title = slga_json.get("title")
        self.description = slga_json.get("description")
        self.license = slga_json.get("license")
        self.source_url = slga_json.get("source_url")
        self.copyright = slga_json.get("copyright")
        self.attribution = slga_json.get("attribution")
        self.crs = slga_json.get("crs")
        self.bbox = slga_json.get("bbox")
        self.resolution_arcsec = slga_json.get("resolution_arcsec")
        self.depth_min = slga_json.get("depth_min")
        self.depth_max = slga_json.get("depth_max")
        self.layers_url = slga_json.get("layers_url")
        self.fetched_files = []

    @retry_decorator()
    def getwcs_slga(self, url, identifier, crs, bbox, resolution, outfname):
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
        resolution = resolution if resolution is not None else self.resolution_arcsec
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
                print(f"WCS data downloaded and saved as {os.path.basename(outfname)}")

        except Exception as e:
            if e.response.status_code == 502:
                logger.error(
                    f"HTTPError 502: Bad Gateway encountered when accessing {url}"
                )
            elif e.response.status_code == 503:
                logger.error(
                    f"HTTPError 503: Service Unavailable encountered when accessing {url}"
                )
            else:
                logger.error(
                    f"Error {e.response.status_code}: {e.response.reason} when accessing {url}"
                )

    def get_slga_layers(
        self,
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
            layernames = layernames if isinstance(layernames, list) else [layernames]
            depth_min = (
                [depth_min] * len(layernames)
                if not isinstance(depth_min, list)
                else depth_min
            )
            depth_max = (
                [depth_max] * len(layernames)
                if not isinstance(depth_max, list)
                else depth_max
            )

            if not (len(depth_min) == len(depth_max) == len(layernames)):
                logger.error("Depth and layer name lists must be of the same length.")

            os.makedirs(outpath, exist_ok=True)

            # If the resolution passed is None, set to native resolution of datasource
            resolution = (
                resolution if resolution is not None else self.resolution_arcsec
            )
            resolution_deg = resolution / 3600.0

            fnames_out = []
            for idx, layername in enumerate(layernames):
                layer_url = self.layers_url[layername]
                # Get depth identifiers for layers
                (
                    identifiers,
                    identifiers_ci_5pc,
                    identifiers_ci_95pc,
                    depth_lower,
                    depth_upper,
                ) = depth2identifier(depth_min[idx], depth_max[idx])

                # if confidence intervals not requested, do this:
                for i in range(len(identifiers)):
                    identifier = identifiers[i]
                    layer_depth_name = (
                        f"SLGA_{layername}_{depth_lower[i]}-{depth_upper[i]}cm"
                    )

                    fname_out = os.path.join(
                        outpath, f"{layer_depth_name}_{property_name}.tiff"
                    )
                    # download data
                    dl = self.getwcs_slga(
                        layer_url, identifier, self.crs, bbox, resolution_deg, fname_out
                    )
                    if dl:
                        fnames_out.append(fname_out)
                # if confidence intervals requested, do this instead:
                if get_ci:
                    for i in range(len(identifiers)):
                        # set identifiers for the 5 and 95% CI's
                        identifier_5 = identifiers_ci_5pc[i]
                        identifier_95 = identifiers_ci_95pc[i]

                        layer_depth_name = (
                            f"SLGA_{layername}_{depth_lower[i]}-{depth_upper[i]}cm"
                        )
                        fname_out_5 = os.path.join(
                            outpath,
                            f"{layer_depth_name}_{property_name}_5percentile.tiff",
                        )

                        fname_out_95 = os.path.join(
                            outpath,
                            f"{layer_depth_name}_{property_name}_95percentile.tiff",
                        )
                        # download data
                        dl_5 = self.getwcs_slga(
                            layer_url,
                            identifier_5,
                            self.crs,
                            bbox,
                            resolution_deg,
                            fname_out,
                        )
                        dl_95 = self.getwcs_slga(
                            layer_url,
                            identifier_95,
                            self.crs,
                            bbox,
                            resolution_deg,
                            fname_out,
                        )
                        if dl_5 and dl_95:
                            fnames_out.append(fname_out_5, fname_out_95)

            return fnames_out
        except Exception as e:
            logger.error(f"Failed to get SLGA layers: {e}")
            return None


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
    except Exception as e:
        logger.error(f"Failed to get identifiers: {e}")
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
    depth_options = [
        "0-5cm",
        "5-15cm",
        "15-30cm",
        "30-60cm",
        "60-100cm",
        "100-200cm",
    ]
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
    except Exception as e:
        logger.error(f"Failed to get min and max depth: {e}")
        return None, None

import logging
import sys
from typing import Any, Dict

import geopandas as gpd

logger = logging.getLogger()

def get_bbox_from_geodf(geojson_data: Dict[str, Any]):
		"""
		Extract the bounding box from a GeoJSON-like dictionary.

		Parameters:
		- geojson_data (dict): The GeoJSON data as a Python dictionary.

		Returns:
		- A list representing the bounding box [min_lon, min_lat, max_lon, max_lat].
		"""
		if "features" not in geojson_data:
				raise ValueError("Input dictionary does not contain 'features' key.")
		
		try:
			gdf = gpd.GeoDataFrame.from_features(geojson_data["features"])
			bbox = list(gdf.total_bounds)
			return bbox
		except Exception as e:
				logger.error("Failed to extract bounding box from GeoJSON data")
				raise ValueError(f"Error processing GeoJSON data: {e}")
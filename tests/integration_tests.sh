#!/usr/bin/env bash
set -ex

pip install .

export DATACUBE_CONFIG_PATH=.test_datacube.conf
export AWS_DEFAULT_REGION=ap-southeast-2
datacube system init
# Add product definitions
# WOfS
datacube metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_albers.odc-product.yaml
datacube product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_filtered_summary.odc-product.yaml
datacube product add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_summary.odc-product.yaml

# Index one WOfS tile
s3-to-dc "s3://dea-public-data/WOfS/filtered_summary/v2.1.0/combined/x_15/y_-40/wofs_filtered_summary_15_-40.yaml" --no-sign-request --skip-lineage wofs_filtered_summary
s3-to-dc "s3://dea-public-data/WOfS/summary/v2.1.0/combined/x_15/y_-40/WOFS_3577_15_-40_summary.yaml" --no-sign-request --skip-lineage wofs_summary

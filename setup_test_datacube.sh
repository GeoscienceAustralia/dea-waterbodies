#!/usr/bin/env bash
set -ex

export DATACUBE_CONFIG_PATH=.test_datacube.conf
export AWS_DEFAULT_REGION=ap-southeast-2
datacube system init
# Add product definitions
# WOfS
datacube metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml
datacube product add https://explorer.sandbox.dea.ga.gov.au/products/wofs_albers.odc-product.yaml
datacube product add https://explorer.sandbox.dea.ga.gov.au/products/wofs_filtered_summary.odc-product.yaml
datacube product add https://explorer.sandbox.dea.ga.gov.au/products/wofs_summary.odc-product.yaml
# Coastline
datacube product add https://explorer.sandbox.dea.ga.gov.au/products/geodata_coast_100k.odc-product.yaml

# Index one WOfS tile (Belconnen)
s3-to-dc 's3://dea-public-data/WOfS/filtered_summary/v2.1.0/combined/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'wofs_filtered_summary'
s3-to-dc 's3://dea-public-data/WOfS/summary/v2.1.0/combined/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'wofs_summary'
# Coastline tile
s3-to-dc 's3://dea-public-data/projects/geodata_coast_100k/v2004/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'geodata_coast_100k'

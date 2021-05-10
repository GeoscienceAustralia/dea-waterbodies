#!/usr/bin/env bash
set -ex

datacube system init
# Add product definitions
# WOfS
https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_albers.odc-product.yaml
https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_filtered_summary.odc-product.yaml

# Index one of each ARD product (5, 7 and 8)
s3-to-dc "s3://dea-public-data/WOfS/filtered_summary/v2.1.0/combined/x_15/y_-40/wofs_filtered_summary_15_-40.yaml" --no-sign-request --skip-lineage wofs_filtered_summary

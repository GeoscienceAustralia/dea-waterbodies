#!/usr/bin/env bash
set -ex

# Setup datacube
docker-compose exec -T index datacube system init --no-default-types --no-init-users
# Setup metadata types
docker-compose exec -T index datacube metadata add "$METADATA_CATALOG"
# Index products we care about for dea-waterbodies
docker-compose exec -T index wget "$PRODUCT_CATALOG" -O product_list.csv
docker-compose exec -T index bash -c "tail -n+2 product_list.csv | grep 'wofs_albers\|wofs_filtered_summary\|wofs_summary' | awk -F , '{print \$2}' | xargs datacube -v product add"
docker-compose exec -T index bash -c "datacube product add https://explorer.sandbox.dea.ga.gov.au/products/geodata_coast_100k.odc-product.yaml"

# Index WOfS and Coastline
cat > index_tiles.sh <<EOF
# Index one WOfS tile (Belconnen)
s3-to-dc 's3://dea-public-data/WOfS/filtered_summary/v2.1.0/combined/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'wofs_filtered_summary'
s3-to-dc 's3://dea-public-data/WOfS/summary/v2.1.0/combined/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'wofs_summary'
# Coastline tile
s3-to-dc 's3://dea-public-data/projects/geodata_coast_100k/v2004/x_15/y_-40/*.yaml' --no-sign-request --skip-lineage 'geodata_coast_100k'
# WOfLs
s3-to-dc 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'wofs_albers'
EOF

cat index_tiles.sh | docker-compose exec -T index bash

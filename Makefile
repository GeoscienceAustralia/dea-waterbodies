build-image:
	docker build . \
		--tag GeoscienceAustralia/dea-waterbodies:test \
		--build-arg ENVIRONMENT=test

build-prod-image:
	docker build . \
		--tag GeoscienceAustralia/dea-waterbodies:latest \
		--build-arg ENVIRONMENT=deployment

run-prod:
	docker run --rm \
		GeoscienceAustralia/dea-waterbodies

test-local:
	pytest tests


# Docker Compose environment
build:
	docker-compose build

up:
	docker-compose up

down:
	docker-compose down

shell:
	docker-compose exec waterbodies bash

test:
	docker-compose exec waterbodies pytest tests

lint:
	docker-compose exec waterbodies black --check dea_waterbodies

integration-test:
	docker-compose up -d
	docker-compose exec -T dea_waterbodies bash ./tests/integration_tests.sh

# C3 Related
initdb:
	docker-compose exec waterbodies \
		datacube system init

metadata:
	docker-compose exec waterbodies \
		datacube metadata add https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/develop/digitalearthau/config/eo3/eo3_landsat_ard.odc-type.yaml

product:
	docker-compose exec waterbodies \
		datacube product add \
        https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_albers.odc-product.yaml \
		https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_filtered_summary.odc-product.yaml \
		https://raw.githubusercontent.com/GeoscienceAustralia/digitalearthau/master/digitalearthau/config/products/wofs_summary.odc-product.yaml

index:
	docker-compose exec waterbodies \
		datacube dataset add --ignore-lineage --confirm-ignore-lineage \
			s3://dea-public-data/WOfS/filtered_summary/v2.1.0/combined/x_15/y_-40/wofs_filtered_summary_15_-40.yaml \
			s3://dea-public-data/WOfS/summary/v2.1.0/combined/x_15/y_-40/WOFS_3577_15_-40_summary.yaml

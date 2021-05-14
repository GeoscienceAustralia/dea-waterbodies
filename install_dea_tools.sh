#!/usr/bin/env bash

git clone --depth 1 --single-branch --no-checkout https://github.com/GeoscienceAustralia/dea-notebooks.git
cd dea-notebooks
git sparse-checkout init
git sparse-checkout set Tools
git checkout develop
git sparse-checkout disable
pip install -e Tools --extra-index-url="https://packages.dea.ga.gov.au"
cd ..

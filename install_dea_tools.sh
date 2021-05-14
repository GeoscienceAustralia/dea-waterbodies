#!/usr/bin/env bash

svn export https://github.com/GeoscienceAustralia/dea-notebooks/trunk/Tools
pip install -e Tools --extra-index-url="https://packages.dea.ga.gov.au"

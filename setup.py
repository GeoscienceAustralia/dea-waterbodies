import io
import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package metadata.
NAME = 'dea-waterbodies'
DESCRIPTION = 'Digital Earth Australia Waterbodies'
URL = 'https://github.com/GeoscienceAustralia/dea-waterbodies'
EMAIL = 'dea@ga.gov.au'
AUTHOR = 'Geoscience Australia'
REQUIRES_PYTHON = '>=3.6.0'

# What packages are required for this module to be executed?
REQUIRED = [
    'datacube', 'geopandas', 'numpy', 'python-geohash'
]

# What packages are optional?
EXTRAS = {
}

# Where are we?
here = os.path.abspath(os.path.dirname(__file__))

# Use the same short and long description.
long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
with open(os.path.join(here, project_slug, '__version__.py')) as f:
    exec(f.read(), about)

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*",
                                    "test", "*.test", "*.test.*", "test.*"]),
    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    # If you change the License, remember to change the Trove Classifier!
    license='Apache 2.0',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Scientific/Engineering :: Hydrology',
    ],
)

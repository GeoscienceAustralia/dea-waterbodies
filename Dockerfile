ARG py_env_path=/env
ARG V_BASE=3.3.0

FROM opendatacube/geobase-builder:${V_BASE} as env_builder
ENV LC_ALL=C.UTF-8

# Install our Python requirements
COPY requirements.txt /conf/
ARG py_env_path
RUN echo "" > /conf/constraints.txt
RUN cat /conf/requirements.txt \
  && env-build-tool new /conf/requirements.txt /conf/constraints.txt ${py_env_path} \
  && rm -rf /root/.cache/pip \
  && echo done

# Below is the actual image that does the running
FROM opendatacube/geobase-runner:${V_BASE}
ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8
    
RUN apt-get update \
    && apt-get install -y \
         libtiff-tools \
         git \
         htop \
         tmux \
         wget \
         curl \
         nano \
         unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

ARG py_env_path
COPY --from=env_builder $py_env_path $py_env_path
ENV PATH="${py_env_path}/bin:${PATH}"

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN echo "Installing dea-waterbodies through the Dockerfile."
RUN pip install --use-feature=2020-resolver --extra-index-url="https://packages.dea.ga.gov.au" .

RUN env && echo $PATH && pip freeze && pip check

# Make sure it's working
RUN waterbodies-ts --version

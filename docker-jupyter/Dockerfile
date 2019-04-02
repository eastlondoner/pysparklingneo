ARG SPARK_DOCKER_IMAGE=notag
FROM $SPARK_DOCKER_IMAGE


RUN rm -f /usr/bin/python \
 && ln -s /usr/bin/pip3 /usr/bin/pip \
 && ln -s /usr/bin/python3 /usr/bin/python

# SciPy
RUN set -ex \
 && buildDeps=' \
    build-essential \
    pkg-config \
    gfortran \
 ' \
 && apt-get update && apt-get install -y --no-install-recommends \
    $buildDeps \
    ca-certificates \
    wget \
    liblapack-dev \
    libopenblas-dev \
 && packages=' \
    scipy \
    jupyter \
    numpy \
 ' \
 && pip3 install --upgrade $packages \
 && rm -rf /root/.cache/pip \
 && apt-get purge -y --auto-remove $buildDeps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:/jupyter/dist/pyspark-neo4j-assembly-0.0.1.jar
# WORKDIR $ZEPPELIN_HOME
# TODO: autogenerate a password and an ssl cert
CMD ["jupyter", "notebook"]
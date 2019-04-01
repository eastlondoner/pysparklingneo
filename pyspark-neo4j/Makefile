SHELL = /bin/bash
VERSION = $(shell cat version.txt)
SCALA_TARGET_VERSION=2.12

.PHONY: clean clean-pyc clean-dist dist test-travis

clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf python/build/
	rm -rf python/*.egg-info
	rm -rf .tox
	rm -rf venv

install-venv:
	test -d venv || virtualenv venv
	
install-neo4j-driver: install-venv
	venv/bin/pip install neo4j

test: test-python test-scala test-integration

test-python:

test-scala:

test-integration: test-integration-matrix test-integration-teardown

test-integration-matrix: \
	install-neo4j-driver \
	test-integration-spark-2.4.0

test-travis: install-neo4j-driver
	$(call test-integration-for-version,$$SPARK_VERSION,$$SPARK_PACKAGE_TYPE)

test-integration-spark-2.4.0:
	$(call test-integration-for-version,2.4.0,hadoop2.8)

define test-integration-for-version
	echo ======================================================================
	echo testing integration with spark-$1
	
	mkdir -p lib && test -d lib/spark-$1-bin-$2 || \
		(pushd lib && curl http://mirrors.ocf.berkeley.edu/apache/spark/spark-$1/spark-$1-bin-$2.tgz | tar xz && popd)
	
	cp log4j.properties lib/spark-$1-bin-$2/conf/

	source venv/bin/activate ; \
		lib/spark-$1-bin-$2/bin/spark-submit \
			--master local[*] \
			--driver-memory 512m \
			--conf spark.neo4j.connection.host="localhost" \
			--jars target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j-assembly-$(VERSION).jar \
			--py-files target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j-assembly-$(VERSION).jar \
			python/pyspark_neo4j/tests.py
			
	echo ======================================================================
endef

dist: clean-pyc install-venv
	sbt -batch assembly
	cd . ; \
    ./venv/bin/pip install -r ./python/deps.txt -t ./deps ; \
    cd deps ; \
    	find . -mindepth 2 -name '*.py' -print | \
    	zip ../target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j-assembly-$(VERSION).jar -@ ; \
    cd .. ; \
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j-assembly-$(VERSION).jar -@


all: lint dist

publish:
	# use spark packages to create the distribution
	sbt -batch spDist

	# push the python source files into the jar
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j_$(SCALA_TARGET_VERSION)-$(VERSION).jar -@

	# copy it to the right name, and update the jar in the zip
	cp target/scala-$(SCALA_TARGET_VERSION)/pyspark-neo4j{_$(SCALA_TARGET_VERSION),}-$(VERSION).jar
	cd target/scala-$(SCALA_TARGET_VERSION) ;\
		zip ../pyspark-neo4j-$(VERSION).zip pyspark-neo4j-$(VERSION).jar

	# send the package to spark-packages
	spark-package publish -c ".sp-creds.txt"  -n "eastlondoner/pyspark-neo4j" -v "$(VERSION)" -f . -z target/pyspark-neo4j-$(VERSION).zip

lint: python-tox scala-style

python-tox: ## check style with flake8
	echo "skipping tox" # tox

scala-style: ## check style with scalastyle
	sbt -batch scalastyle


SHELL:= $(/usr/bin/env bash)

RMR_JAVA=../inst/java

all: $(RMR_JAVA)

$(RMR_JAVA):
	@echo "Making Java libraries with Maven"
	pushd maven
	mvn jar
	cp target/rmr-*.jar ../$(RMR_JAVA)
	popd
	@echo "Done making Java libraries"
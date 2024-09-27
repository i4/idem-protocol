SOURCE_FILES := $(shell find src -name "*.java")
BUILD_MARKER := bin/built

LIBS := lib/eddsa-0.3.0.jar:lib/jackson-core-asl-1.9.4.jar:lib/htrace-core4-4.1.0-incubating.jar
BUILD_DEST := bin/java

.PHONY: all refit clean
.DELETE_ON_ERROR:

all: refit

refit: $(BUILD_MARKER)

bin/built: $(SOURCE_FILES)
	mkdir -p $(BUILD_DEST)
	touch $(BUILD_MARKER)
	javac -d $(BUILD_DEST) -cp $(LIBS) $^
	cp resources/project.properties $(BUILD_DEST)

keys: refit
	mkdir -p scripts/keys
	java -cp $(BUILD_DEST):$(LIBS) refit.crypto.REFITKeyManager

testreplica: refit
	java -cp $(BUILD_DEST):$(LIBS) refit.client.REFITLocalSystem 20

# basic config sanity check
check-config: refit
	java -cp $(BUILD_DEST):$(LIBS) refit.config.REFITConfigTest

clean:
	-rm -rf bin || true

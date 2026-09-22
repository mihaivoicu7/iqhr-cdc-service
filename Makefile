.PHONY: test package image

test:
	./scripts/build.sh

package:
	./scripts/build.sh -DskipTests

image: test
	docker build -t iqhr/iqhr-cdc-service:0.1.0 .

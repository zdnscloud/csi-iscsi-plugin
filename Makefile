# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION=`git describe --tags`
BUILD=`date +%FT%T%z`
BRANCH=`git branch | sed -n '/\* /s///p'`

LDFLAGS=-ldflags "-w -s -X main.version=${VERSION} -X main.build=${BUILD}"
GOSRC = $(shell find . -type f -name '*.go')

REGISTRY_NAME = zdnscloud/iscsi-csi
IMAGE_VERSION = latest

.PHONY: all clean

all: build

build:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o ./iscsi-csi ./cmd/

image:
	docker build -t $(REGISTRY_NAME):$(IMAGE_VERSION) --build-arg version=${VERSION} --build-arg buildtime=${BUILD} .
	docker image prune -f

docker: image
	docker push $(REGISTRY_NAME):$(IMAGE_VERSION)

clean:
	go clean -r -x
	rm -f lvmcsi

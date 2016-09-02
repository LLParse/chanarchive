#!/bin/bash

echo "package version" > "version/version.ignore.go"
echo "" >> "version/version.ignore.go"
echo "func (v *VersionInfo) Populate() {" >> "version/version.ignore.go"
echo "  v.GitHash = \"`git rev-parse HEAD`\"" >> "version/version.ignore.go"
echo "  v.BuildDate = \"`date`\"" >> "version/version.ignore.go"
echo "}" >> "version/version.ignore.go"

#go-bindata -o ./asset/bindata.go -pkg asset asset/...
#go build

# Build linux binary
docker build -t streamingchan -f Dockerfile.build .
docker run --rm -v "$PWD":/go/src/github.com/llparse/streamingchan -w /go/src/github.com/llparse/streamingchan streamingchan go build -v

# Create lightweight container
docker build -t llparse/streamingchan:v1.0 .
docker push llparse/streamingchan:v1.0

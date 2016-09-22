#!/bin/bash -e

echo "package version" > "version/version.ignore.go"
echo "" >> "version/version.ignore.go"
echo "func (v *VersionInfo) Populate() {" >> "version/version.ignore.go"
echo "  v.GitHash = \"`git rev-parse HEAD`\"" >> "version/version.ignore.go"
echo "  v.BuildDate = \"`date`\"" >> "version/version.ignore.go"
echo "}" >> "version/version.ignore.go"

#go-bindata -o ./asset/bindata.go -pkg asset asset/...
#go build

# Build linux binary
docker build -t chanarchive -f Dockerfile.build .
docker run --rm -v "$PWD":/go/src/github.com/llparse/chanarchive -w /go/src/github.com/llparse/chanarchive chanarchive go build -v

# Create lightweight container
docker build -t llparse/chanarchive:v1.0 .
docker push llparse/chanarchive:v1.0

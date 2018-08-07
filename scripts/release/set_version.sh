#!/bin/sh
set -eu -o pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]] ; then
    >&2 echo "Usage: $0 <new_version>"
    exit 1
fi

INPUT_VERSION=$1; shift

MAJOR_VERSION=${INPUT_VERSION%%.*}
WITHOUT_MAJOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.}
MINOR_VERSION=${WITHOUT_MAJOR_VERSION%%.*}
WITHOUT_MINOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.${MINOR_VERSION}.}
PATCH_VERSION=${WITHOUT_MINOR_VERSION%%.*}

XYZ_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

if [[ "${INPUT_VERSION}" == *-* ]] ; then
    TAGS_VERSION="-SNAPSHOT"
else
    TAGS_VERSION=
fi

cd $(dirname -- $0)
cd ${PWD}/../..

# val qdbVersion = "2.8.0-SNAPSHOT"
sed -i -e 's/val qdbVersion\s*=\s*"[0-9.]\+\(-SNAPSHOT\)\?"\s*$/val qdbVersion = "'"${XYZ_VERSION}${TAGS_VERSION}"'"/' build.sbt
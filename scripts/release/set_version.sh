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
sed -i -e 's/val qdbVersion\s*=\s*"[0-9.]*[0-9]\(-SNAPSHOT\)\?"\s*$/val qdbVersion = "'"${XYZ_VERSION}${TAGS_VERSION}"'"/' build.sbt

# pom-java.xml
# <version>2.8.0-SNAPSHOT</version>
# <properties>
#   <groupId>net.quasardb</groupId>
#   <version>2.8.0-SNAPSHOT</version>
#   <file>java/qdb-2.8.0-SNAPSHOT.jar</file>
# </properties>
#   <dependency>
#     <groupId>net.quasardb</groupId>
#     <version>2.8.0-SNAPSHOT</version>
#   </dependency>
#   <dependency>
#     <groupId>net.quasardb</groupId>
#     <version>2.8.0-SNAPSHOT</version>
#   </dependency>

# pom-jni.xml
# <version>2.8.0-SNAPSHOT</version>
# <properties>
#   <groupId>net.quasardb</groupId>
#   <version>2.8.0-SNAPSHOT</version>
#   <file>jni/jni-2.8.0-SNAPSHOT.jar</file>
# </properties>

# pom-jni-arch.xml
# <version>2.8.0-SNAPSHOT</version>
# <properties>
#   <groupId>net.quasardb</groupId>
#   <version>2.8.0-SNAPSHOT</version>
#   <file>jni/jni-2.8.0-SNAPSHOT-${arch}.jar</file>
# </properties>

sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/\(properties\|dependency\)>/ s/<version>[0-9.]*[0-9]\(-SNAPSHOT\)\?<\/version>/<version>'"${XYZ_VERSION}${TAGS_VERSION}"'<\/version>/' pom-java.xml pom-jni.xml pom-jni-arch.xml
sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/properties>/ s/<file>\([-a-zA-Z_/]*\)[0-9.]*[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z]*\)<\/file>/<file>\1'"${XYZ_VERSION}${TAGS_VERSION}"'\3<\/file>/' pom-java.xml pom-jni.xml pom-jni-arch.xml

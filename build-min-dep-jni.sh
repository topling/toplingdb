#!/usr/bin/bash

# ex: topling-8.10.2-frocksdb-1.0, part <frocksdb> will be ignored
if [ -z "${TOPLING_VERSION}" ]; then
    GITHUB_REF=`git symbolic-ref HEAD`
    TOPLING_VERSION=`echo ${GITHUB_REF} | sed -n 's:^refs/tags/topling-'${ROCKSDB_VERSION}'[-_a-z]*\([.0-9]\):\1:p'`
    if [ -z "${TOPLING_VERSION}" ]; then
        echo TOPLING_VERSION is not set and can not parse from HEAD ref >&2
        exit 1
    fi
fi

export USE_LTO=1
export UPDATE_REPO=0
export DEBUG_LEVEL=0
export DISABLE_JEMALLOC=1
export ROCKSDB_DISABLE_GFLAGS=1
export TOPLING_USE_DYNAMIC_TLS=1
export TOPLING_ZIP_TABLE_TRIAL_DAYS=90
MAJOR_DOT_MINOR=`build_tools/version.sh major`.`build_tools/version.sh minor`

make -j60 libsnappy.a liblz4.a libbz2.a
make rocksdbjava install-dcompact -j`nproc` BUILD_PREFIX=min-dep-jni/ \
    PREFIX=min-dep-jni STRIP_DEBUG_INFO=1 ROCKSDB_JAR_WITH_DYNAMIC_LIBS=1

exebin=min-dep-jni/bin/dcompact_worker.exe
patchelf --replace-needed librocksdb.so.${MAJOR_DOT_MINOR} librocksdbjni-linux64.so ${exebin}

ROCKSDB_VERSION=`build_tools/version.sh full`
ROCKSDB_JAVA_VERSION=${ROCKSDB_VERSION}-topling-${TOPLING_VERSION}-trial${TOPLING_ZIP_TABLE_TRIAL_DAYS}
cd java/target
db_artifactId=`sed -n 's/.*<artifactId>\(f\?rocksdbjni\)<\/artifactId>.*/\1/p' ../pom.xml.template`
TARGET_JAR=${db_artifactId}-${ROCKSDB_JAVA_VERSION}.jar
mv rocksdbjni-${ROCKSDB_VERSION}-linux64.jar ${TARGET_JAR}
rm *.sha1
jar -uf ${TARGET_JAR} ../../${exebin}
shasum -a 1 ${TARGET_JAR} > ${TARGET_JAR}.sha1
md5sum      ${TARGET_JAR} > ${TARGET_JAR}.md5

#ospart # e.g. "/centos7"
dir=toplingdb${ospart}/cn/topling/${db_artifactId}/${ROCKSDB_JAVA_VERSION}
for file in ${TARGET_JAR}{,.sha1,.md5} ; do
  ossutil cp --region=cn-qingdao -f $file oss://topling-tools/${dir}/
done
set +x
echo ===========================================
echo ======== Download URL
echo ===========================================
echo https://topling-tools.oss-cn-qingdao.aliyuncs.com/${dir}/${TARGET_JAR}
echo https://topling-tools.oss-cn-qingdao.aliyuncs.com/${dir}/${TARGET_JAR}.sha1
echo https://topling-tools.oss-cn-qingdao.aliyuncs.com/${dir}/${TARGET_JAR}.md5
echo ===========================================

#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GREENPLUM_INSTALL_DIR=/usr/local/gpdb

function load_transfered_bits_into_install_dir() {
  mkdir -p $GREENPLUM_INSTALL_DIR
  tar xzf $TRANSFER_DIR/$COMPILED_BITS_FILENAME -C $GREENPLUM_INSTALL_DIR
}

function configure() {
  pushd gpdb_src
    ./configure --prefix=${GREENPLUM_INSTALL_DIR} --with-gssapi --with-perl --with-python --with-libxml --enable-mapreduce --disable-orca --enable-pxf ${CONFIGURE_FLAGS}
  popd
}

function setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash ubuntu
}

function make_cluster() {
  source "${GREENPLUM_INSTALL_DIR}/greenplum_path.sh"
  export BLDWRAP_POSTGRES_CONF_ADDONS=${BLDWRAP_POSTGRES_CONF_ADDONS}
  # Currently, the max_concurrency tests in src/test/isolation2
  # require max_connections of at least 129.
  export DEFAULT_QD_MAX_CONNECT=150
  export STATEMENT_MEM=250MB
  pushd gpdb_src/gpAux/gpdemo
    su gpadmin -c "source ${GREENPLUM_INSTALL_DIR}/greenplum_path.sh && make create-demo-cluster"
  popd
}

function _main() {
    if [ -z "${MAKE_TEST_COMMAND}" ]; then
        echo "FATAL: MAKE_TEST_COMMAND is not set"
        exit 1
    fi

    time load_transfered_bits_into_install_dir
    time configure
    time setup_gpadmin_user
    time make_cluster

    sudo apt-get -y install wget git && wget https://storage.googleapis.com/golang/go1.10.linux-amd64.tar.gz && tar -xzf go1.10.linux-amd64.tar.gz && sudo mv go /usr/local
    cat > env.sh <<-ENV_SCRIPT
    export GOPATH=/home/gpadmin/go
    source /usr/local/greenplum-db-devel/greenplum_path.sh
    export PGPORT=5432
    export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
    export PATH=\$GOPATH/bin:/usr/local/go/bin:\$PATH
    ENV_SCRIPT

    export GOPATH=/home/gpadmin/go
    chown gpadmin:gpadmin -R $GOPATH
    chmod +x env.sh
    source env.sh
    gpconfig --skipvalidation -c fsync -v off
    gpstop -u

    pushd $GOPATH/src/github.com/greenplum-db/gpbackup
        make depend
        make build
        make integration
        make end_to_end
    popd
}

_main "$@"

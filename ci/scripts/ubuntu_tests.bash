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

function gen_test_script(){
  cat > /opt/run_test.sh <<-EOF
    ROOT_DIR="\${1}"
    RESULT_FILE="/tmp/test_result.log"
    trap look4results ERR
    function look4results() {
      cat "\${RESULT_FILE}"
      exit 1
    }

    SRC_DIR="\${ROOT_DIR}/gpdb_src"
    export GOPATH=\${ROOT_DIR}/go
    export PATH=\$GOPATH/bin:/usr/local/go/bin:\$PATH
    source ${GREENPLUM_INSTALL_DIR}/greenplum_path.sh
    source \${SRC_DIR}/gpAux/gpdemo/gpdemo-env.sh
    pushd \$GOPATH/src/github.com/greenplum-db/gpbackup
        make depend
        make build
        make integration
        make end_to_end
    popd
    cat \${RESULT_FILE}
EOF

	chmod a+x /opt/run_test.sh
}

function run_test_script() {
  su - gpadmin -c "bash /opt/run_test.sh $(pwd)"
}


function _main() {
    time load_transfered_bits_into_install_dir
    time configure
    time setup_gpadmin_user
    time make_cluster

    pushd /tmp
      apt-get -y install wget git && wget https://storage.googleapis.com/golang/go1.10.linux-amd64.tar.gz && tar -xzf go1.10.linux-amd64.tar.gz && mv go /usr/local
    popd
    chown gpadmin:gpadmin -R `pwd`/go
    time gen_test_script
    time run_test_script
}

_main "$@"

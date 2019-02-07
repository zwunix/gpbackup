#!/bin/bash

set -eo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# due to a lack of backwards compatibility in sample_plugin.sh, we need to avoid using the 1.7.1 and previous versions of example_plugin.sh (they don't add a timestamp dir). Thus, we expect the first argument to be a path to sample_plugin.sh with version 0.4.0 or better.
PLUGIN_CONFIG=$1
PLUGIN_BINARY=$2
if [[ "$PLUGIN_BINARY" == "" ]] || [[ "$PLUGIN_CONFIG" == "" ]]; then
  echo "arguments for paths to plugin config and plugin binary are required"
  exit 1
fi

# when using example plugin, it backs up into a constant path
BACKUP_DEST="/tmp/plugin_dest"
TARBALL_DIR=/tmp/generated_tarballs

# sets of flags to test together. first line is blank to indicate NO flags.
# note that incremental is done TWICE, with an additional run added as a special case
# (to capture previous timestamp), and for incremental, old backup(s) are tarred up along with
# the incremental
cat <<SCRIPT > /tmp/flag_sets.txt

--single-data-file
--single-data-file --no-compression
--metadata-only
--no-compression
--single-data-file --leaf-partition-data
--incremental --single-data-file --leaf-partition-data
SCRIPT


function setupPlugin() {
    pluginConfigPath="${PLUGIN_CONFIG}"
    echo "Setting up plugin ${pluginConfigPath} ..."
    pluginExecutablePath="${PLUGIN_BINARY}"
    psql -t postgres -c "SELECT DISTINCT hostname AS string FROM gp_segment_configuration WHERE content != -1" > /tmp/segment-hostnames

    pluginDir=$(dirname ${pluginExecutablePath})
    while read one_host; do
        ssh ${one_host} "mkdir -p ${pluginDir}"
        scp ${pluginExecutablePath} ${one_host}:${pluginExecutablePath} 1>/dev/null
    done </tmp/segment-hostnames
}

function setupDatabase() {
    echo 'Setting up database "testdb"...'
    set +e
        dropdb testdb 2>&1 >/dev/null
    set -e
    createdb testdb
    psql -t testdb -f ${SCRIPT_DIR}/sample_data.sql 2>/dev/null 1>/dev/null
}

function appendDataForIncremental() {
    my_flags=$1
    if [[ ! ${my_flags} == *"incremental"* ]]; then
        # clean any previous backup data, which is already tarred up in another location
        rm -rf ${BACKUP_DEST}
        return
    fi

    if [[ ${my_flags} == *"--from-timestamp"* ]]; then
        psql testdb -c "INSERT into sales VALUES(20, '2017-03-15'::date, 10); INSERT into foo VALUES(2);"
       return
    fi

    psql testdb -c "INSERT into sales VALUES(30, '2017-04-15'::date, 20); INSERT into foo VALUES(3);"
}

function get_tarball_path() {
    # use flags to create name suffix
    my_flags=$1
    my_timestamp=$2

    # beginning of path is fixed:
    filepath=${TARBALL_DIR}/${my_timestamp}-plugin

    # split on space into an array
    IFS=' ' read -ra ARRAY <<< "${my_flags}"
    for word in ${ARRAY[@]}
    do
        suffix=$(echo ${word} | ${SED_BINARY} 's/--//g')
        filepath="${filepath}_${suffix}"
    done
    echo "${filepath}.tar.gz"
}

function do_backup() {
    my_flags=$1 # flag arguments come in as one string with multiple embedded flags
    echo "Backing up with flags: ${my_flags}"

    appendDataForIncremental ${my_flags}

    # note that printf complains in this next line, but it does the right thing to expand without quotes around entire flag set
    set +e
        output=$(gpbackup --dbname testdb --plugin-config ${pluginConfigPath} $(printf "${my_flags}" >/dev/null 2>&1))
        result=$?
    set -e
    if [[ ${result} != 0 ]]; then
        echo "gpbackup failed:"
        echo ${output}
        exit -1
    fi
    timestamp=$(echo ${output} | ${SED_BINARY} -nr 's#.*demoDataDir-1/backups/.*/([0-9]{14})/.*#\1#p')
    echo ${timestamp} > /tmp/timestamp

    pushd ${BACKUP_DEST} 1>/dev/null
        filepath=$(get_tarball_path "${my_flags}" $(cat /tmp/timestamp))
        tar czf ${filepath} .
    popd 1>/dev/null
}


# on macos, need gnu-sed instead of poor, default macos sed
if [[ "${OSTYPE}" == "darwin"* ]] ; then
  SED_BINARY=gsed
  if [[ "$(which ${SED_BINARY})" == "" ]]; then
      echo "please install 'gnu-sed' as 'gsed' via 'brew install gnu-sed'"
      exit 1
  fi
else
  SED_BINARY=sed
fi

rm -rf ${TARBALL_DIR}
mkdir -p ${TARBALL_DIR}
setupPlugin
setupDatabase

# iterate for each flag set; important to delimit by newline char to pick up entire lines
IFS=$'\n'
while read flag_set; do
    do_backup ${flag_set}
done </tmp/flag_sets.txt

# special case:
# run one more backup with flag set for an incremental that requires timestamp of last backup
incr_flags="--incremental --from-timestamp $(cat /tmp/timestamp) --single-data-file --leaf-partition-data"
do_backup "${incr_flags}"

# tar up all artifacts
printf "\nCombining all individual tarball backups at ${TARBALL_DIR} into single tarball...\n"

COMBINED_TARBALL_PATH=/tmp/gpbackup-$(gpbackup --version | awk '{print $NF}')-artifacts.tar.gz
pushd ${TARBALL_DIR} 1>/dev/null
    tar cvzf ${COMBINED_TARBALL_PATH} .
popd 1>/dev/null

printf "\n\nIndividual artifacts have been saved at the following location:\n"
printf "\t${TARBALL_DIR}\n"
printf "and tarred up into a single file:\n"
printf "\t${COMBINED_TARBALL_PATH}\n"
printf "\n##### ##### ##### ##### ##### ##### ##### #####\n\n"

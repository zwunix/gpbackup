#!/bin/sh
#set -e

PRODUCT_NAME=$1
VERSION=$2
if [ -z "${PRODUCT_NAME}" ] || [ -z "${VERSION}" ] ; then
  echo "Missing Arg(s): Please provide the product name and version number:"
  echo "\t ./curl-osl \"<full product name>\" \"<version_number>\""
  exit 1
fi

initial_license_num=339
for ((curr_num=${initial_license_num}; curr_num <= ${initial_license_num}+10; curr_num++)) ; do 
  URL="https://oslo.cfapps.io/osl_reports/${curr_num}"
  echo "Attempting URL: ${URL} \n...\n"
  out=$(curl -s "${URL}" | grep "${PRODUCT_NAME}" | grep "${VERSION}")
  if [ ! -z "${out}" ] ; then 
    echo "\n\nMatch found at URL: ${URL}\n\n"
    exit 0
  fi
done


#!/bin/bash
# Copyright 2021 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file  except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the  License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Entrypoint script for the microlith
# Simple support for dynamic disabling of generic commands and logging
set -e

# Required for legal acceptance
echo "The software referenced by this Docker image includes software from the following under the licenses from those images."
echo "Use of this image and the referenced software is subject to those terms, which can be found in /licenses/"
echo "These can be viewed by running a command like so to provide a custom entrypoint: 'docker run ... cat /licenses/*'"
echo "If the CMOS webserver is running (it is by default), they can also be accessed from '<url>/licenses/' via a browser or curl command."

# Helper function to print our output to entrypoint.log
# Can't use redirection as that would also redirect the output of the processes we spawn into entrypoint.log
function log() {
  if [ "${LOG_TO_STDOUT:-true}" == "true" ]; then
    echo "[ENTRYPOINT] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"
  fi
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") $*" >> /logs/entrypoint.log
}

# Expose all nested config variables to make it simple to see
export PROMETHEUS_CONFIG_FILE=${PROMETHEUS_CONFIG_FILE:-/etc/prometheus/prometheus-runtime.yml}
export PROMETHEUS_CONFIG_TEMPLATE_FILE=${PROMETHEUS_CONFIG_TEMPLATE_FILE:-/etc/prometheus/prometheus-template.yml}
export PROMETHEUS_URL_SUBPATH=${PROMETHEUS_URL_SUBPATH-/prometheus/}
export PROMETHEUS_STORAGE_PATH=${PROMETHEUS_STORAGE_PATH-/prometheus}

export ALERTMANAGER_CONFIG_FILE=${ALERTMANAGER_CONFIG_FILE:-/etc/alertmanager/config.yml}
export ALERTMANAGER_STORAGE_PATH=${ALERTMANAGER_STORAGE_PATH:-/alertmanager}
export ALERTMANAGER_URL_SUBPATH=${ALERTMANAGER_URL_SUBPATH-/alertmanager/}

export LOKI_CONFIG_FILE=${LOKI_CONFIG_FILE:-/etc/loki/local-config.yaml}

export JAEGER_URL_SUBPATH=${JAEGER_URL_SUBPATH-/jaeger}
export JAEGER_CONFIG_FILE=${JAEGER_CONFIG_FILE:-/etc/jaeger/config.json}
export SPAN_STORAGE_TYPE=${SPAN_STORAGE_TYPE:-memory}

export CB_MULTI_ADMIN_USER=${CB_MULTI_ADMIN_USER:-admin}
export CB_MULTI_ADMIN_PASSWORD=${CB_MULTI_ADMIN_PASSWORD:-password}
export CB_MULTI_SQLITE_PASSWORD=${CB_MULTI_SQLITE_PASSWORD:-password}
export CB_MULTI_SQLITE_PATH=${CB_MULTI_SQLITE_PATH:-/data/data.sqlite}
export CB_MULTI_CERT_PATH=${CB_MULTI_CERT_PATH:-/priv/server.crt}
export CB_MULTI_KEY_PATH=${CB_MULTI_KEY_PATH:-/priv/server.key}
export CB_MULTI_UI_PATH=${CB_MULTI_UI_PATH:-/ui}
export CB_MULTI_LOG_LEVEL=${CB_MULTI_LOG_LEVEL:-debug}
export CB_MULTI_BIN=${CB_MULTI_BIN:-/bin/cbmultimanager}
export CB_MULTI_ENABLE_ADMIN_API=${CB_MULTI_ENABLE_ADMIN_API:-true}
export CB_MULTI_ENABLE_CLUSTER_API=${CB_MULTI_ENABLE_CLUSTER_API:-true}
export CB_MULTI_ENABLE_EXTENDED_API=${CB_MULTI_ENABLE_EXTENDED_API:-true}

export CMOS_CFG_BIN=${CMOS_CFG_BIN:-/bin/cmoscfg}
export CMOS_CFG_PATH=${CMOS_CFG_PATH:-/etc/cmos/config.yaml}
export CMOS_CFG_HTTP_PATH_PREFIX=${CMOS_CFG_HTTP_PATH_PREFIX:-/config}
export CMOS_CFG_HTTP_HOST=${CMOS_CFG_HTTP_HOST:-127.0.0.1}
export CMOS_CFG_HTTP_PORT=${CMOS_CFG_HTTP_PORT:-7194}

export CMOS_LOGS_ROOT=${CMOS_LOGS_ROOT:-/logs}

export PROMTAIL_CONFIG_FILE=${PROMTAIL_CONFIG_FILE:-/etc/promtail/config-microlith.yaml}
export PROMTAIL_HTTP_PORT=${PROMTAIL_HTTP_PORT:-9080}
# Temporary, see https://issues.couchbase.com/browse/CMOS-179
if [ "${DISABLE_PROMTAIL:-true}" == "false" ]; then
  unset DISABLE_PROMTAIL
else
  DISABLE_PROMTAIL=true
fi

# Clean up dynamic targets generated
export PROMETHEUS_DYNAMIC_INTERNAL_DIR=${PROMETHEUS_DYNAMIC_INTERNAL_DIR:-/etc/prometheus/couchbase/monitoring/}
rm -rf "${PROMETHEUS_DYNAMIC_INTERNAL_DIR:?}"/
mkdir -p "${PROMETHEUS_DYNAMIC_INTERNAL_DIR}"

if [[ -v "KUBERNETES_DEPLOYMENT" ]]; then
    log "Using Kubernetes mode as KUBERNETES_DEPLOYMENT set (value ignored)"
fi

if [[ ! -x /bin/cbmultimanager ]]; then
    log "Running OSS version, no Couchbase binaries"
    # Simple test this is set rather than value comparison
    export CMOS_OSS_DISTRIBUTION=true
    export CMOS_DISTRIBUTION="OSS"
else
    log "Couchbase binaries available, not OSS version"
    export CMOS_DISTRIBUTION="Couchbase"
fi

# Support passing in custom command to run, e.g. bash
if [[ $# -gt 0 ]]; then
    log "Running custom: $*"
    exec "$@"
else
    for i in /entrypoints/*; do
        EXE_NAME=${i##/entrypoints/}
        UPPERCASE=${EXE_NAME^^}
        DISABLE_VAR=DISABLE_${UPPERCASE%%.*}
        # Set DISABLE_XXX to skip running
        if [[ -v "${DISABLE_VAR}" ]]; then
            log "Disabled as ${DISABLE_VAR} set (value ignored): $i"
        elif [[ -x "$i" ]]; then
            if [ "${LOG_TO_STDOUT:-true}" == "true" ]; then
              log "Running: $i"
              # See https://github.com/hilbix/speedtests for log name pre-pending info
              "$i" "$@" 2>&1 | tee "${CMOS_LOGS_ROOT}/${EXE_NAME}".log | awk '{ print "['"${EXE_NAME}"']" $0 }' &
            else
              log "Running: $i ==> /logs/${EXE_NAME}.log"
              "$i" "$@" &> "${CMOS_LOGS_ROOT}/${EXE_NAME}".log &
            fi
        else
            log "Skipping non-executable: $i"
        fi
    done

    wait -n
fi

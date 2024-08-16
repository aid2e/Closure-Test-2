export PANDA_AUTH=oidc

export PANDA_URL_SSL=https://pandaserver01.sdcc.bnl.gov:25443/server/panda
export PANDA_URL=https://pandaserver01.sdcc.bnl.gov:25443/server/panda
export PANDACACHE_URL=https://pandaserver01.sdcc.bnl.gov:25443/server/panda
export PANDAMON_URL=https://pandamon01.sdcc.bnl.gov
export PANDA_AUTH=oidc
export PANDA_AUTH_VO=EIC
export PANDA_USE_NATIVE_HTTPLIB=1
export PANDA_BEHIND_REAL_LB=1

# export PANDA_AUTH_VO=panda_dev
# export PANDA_AUTH_VO=Rubin:production

export PANDACACHE_URL=$PANDA_URL_SSL

export PANDA_BEHIND_REAL_LB=true
export PANDA_VERIFY_HOST=off

if [ ! -z "$CONDA_PREFIX" ]; then
	export PANDA_SYS=$CONDA_PREFIX
elif [ ! -z "$MAMBA_ROOT_PREFIX" ]; then
        export PANDA_SYS=$MAMBA_ROOT_PREFIX
else
    echo "Need to set PANDA_SYS"
fi
# export PANDA_CONFIG_ROOT=/afs/cern.ch/user/w/wguan/workdisk/iDDS/main/etc/panda/
export PANDA_CONFIG_ROOT=~/.panda/

unset IDDS_HOST
# doma
# export IDDS_HOST=https://aipanda105.cern.ch:443/idds
# export IDDS_HOST=https://aipanda104.cern.ch:443/idds

export IDDS_LOG_LEVEL=debug

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export IDDS_LOG_FILE=${CurrentDir}/idds.log


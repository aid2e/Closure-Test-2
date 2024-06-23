export PANDA_AUTH=oidc
export PANDA_URL_SSL=https://pandaserver-doma.cern.ch/server/panda
export PANDA_URL=https://pandaserver-doma.cern.ch/server/panda
export PANDAMON_URL=https://panda-doma.cern.ch
# export PANDA_AUTH_VO=panda_dev
export PANDA_AUTH_VO=EIC

export PANDACACHE_URL=$PANDA_URL_SSL

# export PANDA_SYS=[your conda dir]
export PANDA_CONFIG_ROOT=~/.panda/

# doma
# export IDDS_HOST=https://aipanda105.cern.ch:443/idds
unset IDDS_HOST

export IDDS_LOG_LEVEL=debug

export IDDS_LOG_FILE=idds.log

export PANDA_BEHIND_REAL_LB=true
export PANDA_VERIFY_HOST=off


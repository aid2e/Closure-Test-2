#!/bin/bash

# passing command to eic-shell does not
# naturally allow passing an argument to the command,
# so some workaround is required

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <float to return>"
    exit 1
fi

# based on Wouter example on mattermost
cat << EOF | $EIC_SHELL_HOME/eic-shell
./eval_command.sh $1
EOF

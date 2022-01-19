#!/bin/bash

echo "Setting Environment Vars."
export GIT_DEV=true
echo "GIT_DEV:"
printenv GIT_DEV

export EXCHANGE_CONFIG_PATH=/Users/Slava/SW_Projects/git/exchanges_data_polling/config/config_exchanges.json
echo "EXCHANGE_CONFIG_PATH:"
printenv EXCHANGE_CONFIG_PATH

export ASSET_CONFIG_PATH=/Users/Slava/SW_Projects/git/exchanges_data_polling/config/config_asset_mapping.json
echo "ASSET_CONFIG_PATH:"
printenv ASSET_CONFIG_PATH


#!/usr/bin/env bash
export REVIEWABLE_FIREBASE_URL="https://reviewable-zoox-enterprise.firebaseio.com"
export REVIEWABLE_FIREBASE_CREDENTIALS_FILE="./firebase.credentials.json"
export REVIEWABLE_ENCRYPTION_AES_KEY="$(cat encryption_aes_key.txt)"

if [[ $# -eq 0 ]]; then
echo "Specify one PR key"
exit
fi
fetches=$1

fetch_data () {
  results=$(mktemp)
  node read.js --path $* > $results
  jq -r . < $results
  rm $results
}

fetch_data reviews/$fetches

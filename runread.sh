#!/usr/bin/env bash
export REVIEWABLE_FIREBASE_URL="https://reviewable-zoox-enterprise.firebaseio.com"
export REVIEWABLE_FIREBASE_CREDENTIALS_FILE="./firebase.credentials.json"
export REVIEWABLE_ENCRYPTION_AES_KEY="$(cat encryption_aes_key.txt)"

if [[ $# -eq 0 ]]; then
echo "Specify how many PRs to harvest"
exit
fi
fetches=$1

fetch_data () {
  results=$(mktemp)
  node read.js --path $* > $results
  jq -r . < $results
  rm $results
}

fetch_data repositories/zooxco/driving/pullRequests > pr_list.json
cut -f4 -d\" pr_list.json | grep -v "{\|}" | tail -25000 | head -$fetches > pr_list.txt
echo '"reviewKey","pullRequestId","pullRequestCreationTimestamp","firstCommit","state","lastRevisionKey","baseBranch","firstReview","lastReview"'
while read review_key; do
  echo -n "${review_key},"
  echo -n "$(
    fetch_data reviews/$review_key | jq -r '[
      .core.pullRequestId,
      .core.pullRequestTimestamp,
      .revisions.r1.commitTimestamp,
      .core.state,
      .core.lastRevisionKey,
      .core.baseBranch
      ] | @csv'
      ),"
echo "$(
    fetch_data reviews/$review_key | jq -r '[
      [.revisions[].captureTimestamp] | min, max
    ] | @csv'
    )"
  done < pr_list.txt

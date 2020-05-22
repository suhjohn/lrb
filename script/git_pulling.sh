#!/bin/bash

while [[ 1 ]]; do
  git remote update
  UPSTREAM=${1:-'@{u}'}
  LOCAL=$(git rev-parse @)
  REMOTE=$(git rev-parse "$UPSTREAM")
  BASE=$(git merge-base @ "$UPSTREAM")

  if [ $LOCAL = $REMOTE ]; then
      echo "Up-to-date"
  elif [ $LOCAL = $BASE ]; then
      echo "Need to pull"
      echo "pulling"
      git pull
      pip3 install -r /proj/cops-PG0/workspaces/ssuh/lrb/pywebcachesim/requirements.txt
      /proj/cops-PG0/workspaces/ssuh/lrb/setup.sh
      python3 /proj/cops-PG0/workspaces/ssuh/lrb/script/run.py
  elif [ $REMOTE = $BASE ]; then
      echo "Need to push"
  else
      echo "Diverged"
  fi
  sleep 30
done
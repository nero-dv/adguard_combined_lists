#!/usr/bin/env bash

cd /home/$USER/scripts/adguard_combined_lists

# Check for changes
if git diff --quiet adguard_combined_list.txt; then
    echo "No changes detected. Skipping upload."
    exit 0
fi

# Stage, commit, and push if there are changes
git add adguard_combined_list.txt
git commit -m "Auto-update list"

GITHUB_PAT=$(cat ~/.github_pat)

git push https://$GITHUB_PAT@github.com/nero-dv/adguard_combined_lists.git main

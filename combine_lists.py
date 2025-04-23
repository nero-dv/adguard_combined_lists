#!/usr/bin/env python3

import requests

# List of URLs for the .txt files to download.
urls = [
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_30.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_11.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_50.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_3.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_44.txt",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.plus.txt",
    "https://easylist.to/easylist/easyprivacy.txt"
]

unique_lines = set()
output_lines = []

for url in urls:
    try:
        response = requests.get(url)
        response.raise_for_status()
        text = response.text
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        continue

    for line in text.splitlines():
        if line not in unique_lines:
            unique_lines.add(line)
            output_lines.append(line)
        else:
            pass

output_filename = "/home/lemon/scripts/adguard_combined_lists/adguard_combined_list.txt"
with open(output_filename, "w", encoding="utf-8") as f:
    for line in output_lines:
        f.write(line + "\n")

print(f"Processing complete. Combined output written to '{output_filename}'.")

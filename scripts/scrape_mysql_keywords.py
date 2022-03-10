# Scrape MySQL documentation webpage for all keywords, and split into reserved and non-reserved keywords
import re
import requests
from bs4 import BeautifulSoup

# Get the html content into bs4
r = requests.get("https://dev.mysql.com/doc/refman/8.0/en/keywords.html")
soup = BeautifulSoup(r.content, features="html.parser")

# Get the div with all the keywords
keyword_div = soup.find("div", {"class": "simplesect"})

# Extract each list item
keyword_list_items = keyword_div.find_all("li", {"class": "listitem"})

# Extract the actual word out
reserved_keywords = []
non_reserved_keywords = []
for li in keyword_list_items:
    code = li.find("code")
    keyword = code.string
    if "(R)" in li.find("p").text:
        reserved_keywords.append(keyword)
    else:
        non_reserved_keywords.append(keyword)

# Print results
print("Reserved Keywords:")
for word in reserved_keywords:
    print('\t' + word)
print("Non-Reserved Keywords:")
for word in non_reserved_keywords:
    print('\t' + word)

# Print useful stats
print("Number of Reserved Keywords:", len(reserved_keywords))
print("Number of Non-Reserved Keywords:", len(non_reserved_keywords))
print("Total keywords:", len(reserved_keywords) + len(non_reserved_keywords))

# Save to file
with open("reserved_keywords.txt", 'w') as f:
    f.write(",".join(reserved_keywords))
with open("non_reserved_keywords.txt", 'w') as f:
    f.write(",".join(non_reserved_keywords))
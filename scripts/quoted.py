# This is just so it's easy to copy paste to vitess
# Open list of reserved and keywords
with open("reserved_keywords.txt", "r") as f:
    reserved_keywords = f.read().split(',')
    f.close()

with open("quoted_reserved_keywords.txt", 'w') as f:
    f.write(",\n".join(["\"" + x.lower() + "\"" for x in reserved_keywords]))
    f.close()

# Open list of non-reserved keywords
with open("non_reserved_keywords.txt", "r") as f:
    non_reserved_keywords = f.read().split(',')
    f.close()

with open("quoted_non_reserved_keywords.txt", 'w') as f:
    f.write(",\n".join(["\"" + x.lower() + "\"" for x in non_reserved_keywords]))
    f.close()
# !/bin/bash

underscore_count=0
dash_count=0

for file in *.txt; do
    reading_flags=0

    while IFS= read -r line; do
        if [[ "$line" == "Flags:" || "$line" == "Usage of vtctlclient:" ]]; then
            reading_flags=1
            continue
        fi

        if [[ $reading_flags -eq 1 ]]; then
            flag=$(echo "$line" | awk '{print $1}')

            if [[ "$flag" == --* ]]; then
                flag_name="${flag:2}"

                [[ "$flag_name" == *_* ]] && ((underscore_count++))
                [[ "$flag_name" == *-* ]] && ((dash_count++))
            fi
        fi
    done < "$file"
done

echo "-------------------------------------"
echo "Total Flags with underscores (_): $underscore_count"
echo "Total Flags with dashes (-): $dash_count"



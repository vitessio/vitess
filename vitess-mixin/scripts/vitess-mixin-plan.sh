#!/bin/bash
if [[ -z "$ENV" || ("${ENV}" != 'dev'  && "${ENV}" != 'prod') ]]; then
    echo -e "##ERROR\nPlease specify ENV (prod or dev)\nUsage: ENV='prod' ./vitess-mixin-plan.sh" 1>&2
    exit 1
fi

export ENVIRONMENT=$ENV

EXIT_STATUS=0;

echo "#### Building origin/main"
REPODIR="$(pwd)"
TEMPDIR="$(mktemp -d)"

cd $TEMPDIR
git clone git@github.com:vitessio/vitess.git  > /dev/null 2>&1
cd vitess/vitess-mixin
jb install > /dev/null 2>&1
make dashboards_out > /dev/null 2>&1
make prometheus_rules.yaml  > /dev/null 2>&1
# TODO enalbe when alerts are not empty.
# make prometheus_alerts.yaml > /dev/null 2>&1

echo -e "\nDone!\n"

cd $REPODIR
make dashboards_out > /dev/null 2>&1
make prometheus_rules.yaml  > /dev/null 2>&1
# TODO enalbe when alerts are not empty.
# make prometheus_alerts.yaml > /dev/null 2>&1

branch=$(git rev-parse --abbrev-ref HEAD)
echo -e "#### Diff origin/main with $branch:\n"

# TODO check prometheus_alerts.yaml

t="# Checking prometheus_rules.yaml...";
d=$(diff -urt --label origin/main/prometheus_rules.yaml "$TEMPDIR/vitess/vitess-mixin/prometheus_rules.yaml" --label $branch/prometheus_rules.yaml "prometheus_rules.yaml" 2>/dev/null)
if [ "$?" = "0" ];
then
  echo $t OK
else
  echo $t NOK
  echo "$d"
  EXIT_STATUS=2
fi

DASHBOARDS=()
for filename in $(ls dashboards_out)
do
    t="# Checking $filename..."
    DASHBOARDS+=($filename)
    d=$(diff -urt --label origin/main/$filename "$TEMPDIR/vitess/vitess-mixin/dashboards_out/"$filename --label $branch/$filename "dashboards_out/"$filename 2>/dev/null)
    if [ "$?" = "0" ];
    then
      echo $t OK
    else
      if [ -e "$TEMPDIR/vitess-mixin/dashboards_out/"$filename ];
      then
        echo $t NOK
        echo "$d"
      else
        echo $t "This is a new dashboard not present in origin/main" NOK
      fi
      EXIT_STATUS=2
    fi
done

for filename in $(ls $TEMPDIR/vitess/vitess-mixin/dashboards_out)
do
  t="# Checking $filename..."
  if [[ ! " ${DASHBOARDS[*]} " == *"$filename"* ]];
  then
    echo $t This dashboard has been removed NOK ;
    EXIT_STATUS=2
  fi
done

echo -e "\nEXIT STATUS:"
if [ "$EXIT_STATUS" -eq 0 ]; then
  echo -e "✅ Your dashboards local version matches the origin/main version"
elif [ "$EXIT_STATUS" -eq 2 ]; then
  echo -e "👀 If you are happy with your changes open a PR"
else
  echo "❌"
  exit 1
fi

rm -rf $TEMPDIR

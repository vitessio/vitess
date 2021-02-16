// Re-cyclable components for text resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local text = grafana.text;

// TODO: figure out how to make emoji works in jsonnet. They are not correctly handled
{

  local footnote =
    |||
      This Dasboard has been automatically generated using vitess-mixin.
      If you want to contribute please visit [https://github.com/vitess/vitess-mixin](https://github.com/vitessio/vitess/tree/master/vitess-mixin)!
    |||,


  local drill_down_note = 'Please use the data links in each panel to zoom into a specific tier or dimension.',

  local notes = {
    footnote: footnote,
    drill_down_note: drill_down_note,
  },

  clusterOverview::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### Cluster overview

        This is a general overview of our Vitess clusters where only drilldown by region is allowed. %(drill_down_note)s

        %(footnote)s
      ||| % notes
    ),


  keyspaceOverview::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### Keyspace overview

        This is a general overview of our keyspaces where only drilldown by region, keyspace and table is allowed. %(drill_down_note)s

        %(footnote)s
      ||| % notes
    ),


  vtgateOverview::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vtgate overview

        This is a general overview of our vtgate tier where only drilldown by region is allowed. %(drill_down_note)s

        %(footnote)s
      ||| % notes
    ),

  vtgateHost::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vtgate host

        This is a detailed view of our vtgate hosts with drilldowns by host.

        %s
      ||| % footnote,
    ),

  vttabletHost::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vttablet host

        This is a detailed view of our vttablet hosts with drilldowns by keyspace, shard and host.

        %s
      ||| % footnote,
    ),
}

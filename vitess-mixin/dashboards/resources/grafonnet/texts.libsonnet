// Re-cyclable components for text resources
local config = import '../../../config.libsonnet';
local grafana = import '../../../vendor/grafonnet/grafana.libsonnet';

local text = grafana.text;

// TODO: figure out how to make emoji work in jsonnet. They are not correctly handled
{

  local footnote =
    |||
      This Dasboard has been automatically generated using vitess-mixin.
      If you want to contribute please visit [https://github.com/vitess/vitess-mixin](https://github.com/vitessio/vitess/tree/main/vitess-mixin)!
    |||,

  local notes = {
    footnote: footnote,
  },

  clusterOverview::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### Cluster overview

        This is a general overview of the Vitess clusters.

        %(footnote)s
      ||| % notes
    ),

  vtgateOverview::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vtgate overview

        This is a general overview of the vtgate tier.

        %(footnote)s
      ||| % notes
    ),

  vtgateHost::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vtgate host
        %s
      ||| % footnote,
    ),

  vttabletHost::
    text.new(
      '',
      mode='markdown',
      content=|||
        #### vttablet host
        %s
      ||| % footnote,
    ),
}

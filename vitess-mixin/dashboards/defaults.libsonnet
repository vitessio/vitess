{
  local grafanaDashboards = super.grafanaDashboards,

  grafanaDashboards:: {
    [filename]: grafanaDashboards[filename] {
      // Modify tooltip to only show a single value
      rows: [
        row {
          panels: [
            panel {
              tooltip+: {
                shared: false,
              },
            }
            for panel in super.panels
          ],
        }
        for row in super.rows
      ],

    }
    for filename in std.objectFields(grafanaDashboards)
  },
}

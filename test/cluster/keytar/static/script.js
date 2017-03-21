$(document).ready(function() {
  var resultsElement = $("#test-results");

  var appendTestResults = function(data) {
    resultsElement.empty();
    var html = " \
        <table align='center' id='results' border='1'> \
        <thead> \
          <tr> \
            <th>Time</th> \
            <th>Docker Image</th> \
            <th>Sandbox Name</th> \
            <th>Status</th> \
            <th>Tests</th> \
            <th>Results</th> \
          </tr> \
        </thead> \
        <tbody>";
    $.each(data, function(key, value) {
      html += "<tr><td>" + value.timestamp + "</td><td><a href=https://hub.docker.com/r/" + value.docker_image + ">" + value.docker_image + "</a></td><td>" + value.name + "</td><td>" + value.status + "</td><td><table>";
      $.each(value.tests, function(key, val) {
        html += "<tr><td>" + key + "</td></tr>";
      });
      html += "</table></td><td><table>";
      $.each(value.tests, function(key, val) {
        html += "<tr><td><a href='test_log?log_name=" + value.timestamp + "_" + key + "'>" + val + "</a></td></tr>";
      });
      html += "</table></td></tr>";
    });
    html += "</tbody></table>";
    resultsElement.append(html);
  };

  // Poll every second.
  var fetchTestResults = function() {
    $.getJSON("/test_results").done(appendTestResults).always(
      function() {
        setTimeout(fetchTestResults, 60000);
      });
  };
  fetchTestResults();
});

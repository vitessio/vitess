$(document).ready(function() {
  var headerTitleElement = $("#header h1");
  var entriesElement = $("#guestbook-entries");
  var formElement = $("#guestbook-form");
  var submitElement = $("#guestbook-submit");
  var entryContentElement = $("#guestbook-entry-content");
  var hostAddressElement = $("#guestbook-host-address");

  // Look for page number in the URL, if any.
  var pageNum = -1;
  var match = window.location.href.match(/\/page\/(\d+)$/);
  if (match) {
    pageNum = parseInt(match[1]);
    headerTitleElement.text('Guestbook Page ' + pageNum);
  } else {
    // No page number provided. Link to a random one.
    pageNum = Math.floor(Math.random() * 100);
    entriesElement.html('<p>Pick a page to view by navigating to <code>/page/###</code> or go to a <a href="/page/' + pageNum + '">random page</a>.</p>');
    formElement.remove();
    return;
  }

  var appendGuestbookEntries = function(data) {
    entriesElement.empty();
    $.each(data, function(key, val) {
      entriesElement.append("<p>" + val + "</p>");
    });
  }

  var handleSubmission = function(e) {
    e.preventDefault();
    var entryValue = entryContentElement.val()
    if (entryValue.length > 0) {
      entriesElement.append("<p>...</p>");
      $.getJSON("/rpush/guestbook/" + pageNum + "/" + entryValue, appendGuestbookEntries);
    }
    return false;
  }

  // colors = purple, blue, red, green, yellow
  var colors = ["#549", "#18d", "#d31", "#2a4", "#db1"];
  var randomColor = colors[Math.floor(5 * Math.random())];
  (function setElementsColor(color) {
    headerTitleElement.css("color", color);
    entryContentElement.css("box-shadow", "inset 0 0 0 2px " + color);
    submitElement.css("background-color", color);
  })(randomColor);

  submitElement.click(handleSubmission);
  formElement.submit(handleSubmission);
  hostAddressElement.append(document.URL);

  // Poll every second.
  (function fetchGuestbook() {
    $.getJSON("/lrange/guestbook/" + pageNum).done(appendGuestbookEntries).always(
      function() {
        setTimeout(fetchGuestbook, 1000);
      });
  })();
});

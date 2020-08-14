
$(document).ready(function () {
    $('button[data-btn=discover-instance]').unbind("click");
    $('button[data-btn=discover-instance]').click(function() {

        if (!$("#discoverHostName").val()) {
            return addAlert("You must enter a host name");
        }
        if (!$("#discoverPort").val()) {
            return addAlert("You must enter a port");
        }
        discover($("#discoverHostName").val(), $("#discoverPort").val())
        return false;
    });
    $("#discoverHostName").focus();
});

function discover(hostname, port) {
    showLoader();
    var uri = "/api/discover/"+hostname+"/"+port;
    $.get(appUrl(uri), function (operationResult) {
        hideLoader();
        if (operationResult.Code == "ERROR" || operationResult.Details == null) {
            addAlert(operationResult.Message)
        } else {
        	var instance = operationResult.Details;
            addInfo('Discovered <a href="' + appUrl('/web/search?s='+instance.Key.Hostname+":"+instance.Key.Port) + '" class="alert-link">'
            		+instance.Key.Hostname+":"+instance.Key.Port+'</a>'
            	);
        }   
    }, "json"); 
	
}
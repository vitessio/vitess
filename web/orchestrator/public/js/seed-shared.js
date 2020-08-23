
function appendSeedDetails(seed, selector) {    	
	var row = '<tr>';
	var statusMessage;
	if (seed.IsComplete) {
		statusMessage = (seed.IsSuccessful ? '<span class="text-success">Success</span>' : '<span class="text-danger">Fail</span>');
	} else {
		statusMessage = '<span class="text-info">Active</span>';
	}
	row += '<td>' + statusMessage + '</td>';
	row += '<td><a href="' + appUrl('/web/seed-details/' + seed.SeedId) + '">' + seed.SeedId + '</a></td>';
	row += '<td><a href="' + appUrl('/web/agent/'+seed.TargetHostname) + '">'+seed.TargetHostname+'</a></td>';
	row += '<td><a href="' + appUrl('/web/agent/'+seed.SourceHostname) + '">'+seed.SourceHostname+'</a></td>';
	row += '<td>' + seed.StartTimestamp + '</td>';
	row += '<td>' + (seed.IsComplete ? seed.EndTimestamp : '<button class="btn btn-xs btn-danger" data-command="abort-seed" data-seed-source-host="'+seed.SourceHostname+'" data-seed-target-host="'+seed.TargetHostname+'" data-seed-id="' + seed.SeedId + '">Abort</button>') + '</td>';
	row += '</tr>';
	$(selector).append(row);
    hideLoader();
}

function appendSeedState(seedState) {    	
	var action = seedState.Action;
	action = action.replace(/Copied ([0-9]+).([0-9]+) bytes (.*$)/, function(match, match1, match2, match3) { 
		return "Copied " + toHumanFormat(match1) + " / " + toHumanFormat(match2) + " " + match3;
	});
	var row = '<tr>';
	row += '<td>' + seedState.StateTimestamp + '</td>';
	row += '<td>' + action + '</td>';
	row += '<td>' + seedState.ErrorMessage + '</td>';
	row += '</tr>';
	$("[data-agent=seed_states]").append(row);
    hideLoader();
}

$("body").on("click", "button[data-command=abort-seed]", function(event) {
	var seedId = $(event.target).attr("data-seed-id");
	var sourceHost = $(event.target).attr("data-seed-source-host");
	var targetHost = $(event.target).attr("data-seed-target-host");

	var message = "Are you sure you wish to abort seed " + seedId + " from <code><strong>" + 
		sourceHost + "</strong></code> to <code><strong>" + 
		targetHost + "</strong></code> ?";
	bootbox.confirm(message, function(confirm) {
		if (confirm) {
	    	showLoader();
	        $.get(appUrl("/api/agent-abort-seed/"+seedId), function (operationResult) {
				hideLoader();
				if (operationResult.Code == "ERROR") {
					addAlert(operationResult.Message)
				} else {
					location.reload();
				}	
	        }, "json");
		}
	});
});

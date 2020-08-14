
$(document).ready(function () {
    showLoader();
    
    $.get(appUrl("/api/seeds"), function (seeds) {
	        showLoader();
	        var hasActive = false;
	        seeds.forEach(function (seed) {
	    		appendSeedDetails(seed, "[data-agent=seed_details]");
	    		if (!seed.IsComplete) {
	    			hasActive = true;
	    		}
	    	});
    		if (hasActive) {
    			activateRefreshTimer();
    		}
	    }, "json");
});	

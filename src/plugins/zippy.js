var page = require('webpage').create(),
    system = require('system'),
    url = system.args[1];
    

page.onError = function(msg,trace) {
// do not log JS errors
//console.log('Error: '+msg);
}

page.settings.resourceTimeout = 60000;

page.open(url, function (status) {
   
    var result=page.evaluate( function () { 
    	try {
    	return document.getElementById('dlbutton').href;
    	} catch (e) {
    	return  document.querySelector('img[src="/images/download.png"]').parentElement.href
    	}
    }); 
    if (result) {
    	console.log(result);
    	phantom.exit(0);
    } else {
    	phantom.exit(1);
    }
});

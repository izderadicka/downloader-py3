var page = require('webpage').create(),
    system = require('system'),
    url = system.args[1];
    

page.onError = function(msg,trace) {
// do not log JS errors
//console.log('Error: '+msg);
}



page.open(url, function (status) {
   
    var result=page.evaluate( function () { return document.getElementById('dlbutton').href}); 
    console.log(result);
    phantom.exit(0);
});

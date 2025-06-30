//Class to encapsulate a user, providing
//access to the user's roles on the server.
function User() {
	this.ip = "";
	this.name = "";
	this.roles = new Object();
	this.isLoggedIn = false;
	this.isLocal = false;
	this.usersClass = true;
	this.usersClassIsXMLFile = true;
	this.loginURL = "";
	this.logoutURL = "";

	var req = new AJAX();
	req.GET("/user", req.timeStamp(), null);
	if (req.success()) {
		var xml = req.responseXML();
		var root = xml.documentElement;
		this.ip = root.getAttribute("ip");
		this.name = root.getAttribute("name");
		this.isLocal = (root.getAttribute("location") == "local");
		this.isLoggedIn = (this.name != "");
		this.usersClass = root.getAttribute("usersClass");
		this.usersClassIsXMLFile = (root.getAttribute("usersClassIsXMLFile") == "yes");
		this.loginURL = root.getAttribute("loginURL");
		this.logoutURL = root.getAttribute("logoutURL");

		var roleElements = root.getElementsByTagName("role");
		for (var i=0; i<roleElements.length; i++) {
			var role = roleElements[i].firstChild;
			if (role) {
				var roleName = role.nodeValue.replace(/^\s+|\s+$/g,"");
			}
			this.roles[roleName] = true;
		}
	}
	this.hasRole = function(role) {
		return (this.roles[role] != null);
	}
}

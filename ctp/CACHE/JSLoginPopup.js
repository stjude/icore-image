//Function to provide a login popup with a
//redirector after it completes.
function showLoginPopup(url) {

	var w = 300;
	var h = 235;
	var closebox = "/icons/closebox.gif";
	var id = "loginPopupID";

	var div = document.getElementById(id);
	if (div) div.parentNode.removeChild(div);

	var div = document.createElement("DIV");
	div.id = "loginPopupContentID";
	div.className = "content";
	div.redirectURL = url;

	var center = document.createElement("CENTER");

	var table = document.createElement("TABLE");
	var tbody = document.createElement("TBODY");
	tbody.appendChild(getLoginRow("Username:", "text", "username"));
	tbody.appendChild(getLoginRow("Password:", "password", "password"));
	table.appendChild(tbody);
	center.appendChild(table);

	div.appendChild(center);

	showDialog(id, w, h, "Login", closebox, "Login", div, loginPopupOK, null);
	document.getElementById("username").focus();

	function getLoginRow(heading, type, inputID) {
		var tr = document.createElement("TR");
		var td = document.createElement("TD");
		td.appendChild(document.createTextNode(heading));
		tr.appendChild(td);
		td = document.createElement("TD");
		var input = document.createElement("INPUT");
		input.type = type;
		input.id = inputID;
		input.name = inputID;
		input.style.width = 150;
		input.onkeydown = loginKeyDown;
		td.appendChild(input);
		tr.appendChild(td);
		return tr;
	}
}

function loginKeyDown(event) {
	event = getEvent(event);
	source = getSource(event);
	var kc = event.keyCode;
	if (kc == 13) {
		var username = document.getElementById("username");
		var password = document.getElementById("password");
		if (source === username) {
			event.cancelBubble = true;
			password.focus();
		}
		else if (source === password) loginPopupOK()
	}
}

function loginPopupOK() {
	var username = encodeURIComponent(document.getElementById("username").value);
	var password = encodeURIComponent(document.getElementById("password").value);
	var url = document.getElementById("loginPopupContentID").redirectURL;

	var qs = "username="+username+"&password="+password+"&url="+url+"&timeStamp="+new Date().getTime();
	openURL("/login?"+qs, "_self");
}

function logout(url) {
	openURL("/login?logout=true&url="+url, "_self");
}


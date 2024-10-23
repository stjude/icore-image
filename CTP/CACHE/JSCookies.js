//Functions for setting and getting cookies
function setCookie(name ,value) {
	var path = ";path=/";
	var nextyr = new Date();
	nextyr.setFullYear(nextyr.getFullYear() + 1);
	var expires = ";expires="+nextyr.toGMTString();
	document.cookie = name + "=" + encodeURIComponent(value) + path + expires;
}

function setSessionCookie(name ,value) {
	var path = ";path=/";
	document.cookie = name + "=" + encodeURIComponent(value) + path;
}

function getCookieObject() {
	var cookies = new Object();
	var allcookies = document.cookie;
	var cooks = allcookies.split(";");
	for (var i=0; i<cooks.length; i++) {
		cooks[i] = cooks[i].replace(/\s/g,"");
		var cook = cooks[i].split("=");
		cookies[cook[0]] = decodeURIComponent(cook[1]);
	}
	return cookies;
}

function clearCookie(name, cookies) {
	var cooks = (cookies != null) ? cookies : getCookieObject();
	if (cooks[name] != null) {
		var date = new Date();
		date.setFullYear(1980);
		var expires = ";expires="+date.toGMTString();
		document.cookie = name + "=" + "x" + expires;
	}
}

function getCookie(name, cookies) {
	var cooks = (cookies != null) ? cookies : getCookieObject();
	var cook = cooks[name];
	return (cook != null) ? cook : "";
}

function listCookies() {
	var cooks = getCookieObject();
	var text = "";
	for (x in cooks) {
		text += x + "=" + cooks[x] + "\n";
	}
	alert("Cookies:\n"+text);
}

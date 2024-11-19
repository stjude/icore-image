//This function returns a standard XMLHttpRequest, if
//one is available (IE7 or later, Chrome, and Firefox).
//Note: The API for this object is different from the one
//for the AJAX object below because the responseText and
//responseXML properties are available directly in this
//object, but they are only available as function calls
//in the AJAX object.
function getXmlHttp() {
	var req = null;

	if (window.XMLHttpRequest) {
		try { req = new XMLHttpRequest(); }
		catch (unableXMLHttpRequest) {
			alert("Unable to instantiate XMLHttpRequest");
		}
	}

	if (req) {
		req.success = function() { return req.isReady() && req.isOK(); };
		req.isReady = function() { return (req.readyState == 4); };
		req.isOK = function() { return (req.status == 200); };
		req.timeStamp = function() { return "timeStamp=" + new Date().getTime(); };

		req.GET =
			function(url, qs, handler) {
				var async = (handler != null);
				req.open("GET", url + (qs ? "?"+qs : ""), async);
				if (async) req.onreadystatechange = function() { handler(req); };
				req.send(null);
			};

		req.DELETE =
			function(url, qs, handler) {
				var async = (handler != null);
				req.open("DELETE", url + (qs ? "?"+qs : ""), async);
				if (async) req.onreadystatechange = function() { handler(req); };
				req.send(null);
			};

		req.POST =
			function(url, qs, handler, contentType) {
				req.open("POST", url, true);
				if (handler) req.onreadystatechange = function() { handler(req); };
				contentType = contentType ? contentType : "application/x-www-form-urlencoded";
				req.setRequestHeader("Content-Type", contentType);
				req.send(qs);
			};

		req.PUT =
			function(url, qs, handler, contentType) {
				req.open("PUT", url, true);
				if (handler) req.onreadystatechange = function() { handler(req); };
				contentType = contentType ? contentType : "application/x-www-form-urlencoded";
				req.setRequestHeader("Content-Type", contentType);
				req.send(qs);
			};
	}
	return req;
}

//This is an object that encapsulates an AJAX request.
//It works with both the standard XMLHttpRequest and
//a Microsoft ActiveXObject implementation of XMLHTTP.
function AJAX() {
	this.req = null;
	this.type = "";

	if (!this.req && window.ActiveXObject) {
		try {
			this.req = new ActiveXObject("Microsoft.XMLHTTP");
			this.type = "ActiveXObject";
		}
		catch (unableMicrosoft) { }
	}

	if (!this.req && window.XMLHttpRequest) {
		try {
			this.req = new XMLHttpRequest();
			this.type = "XMLHttpRequest";
		}
		catch (unableXMLHttpRequest) { }
	}

	if (this.req) {

		this.success = function() { return this.isReady() && this.isOK(); };
		this.isReady = function() { return this.req.readyState == 4 };
		this.isOK = function() { return (this.req.status == 200); };
		this.timeStamp = function() { return "timeStamp=" + new Date().getTime(); };

		this.GET =
			function(url, qs, handler) {
				var thisObject = this;
				var async = (handler != null);
				this.req.open("GET", url + (qs ? "?"+qs : ""), async);
				if (async) this.req.onreadystatechange = function() {
					if (thisObject.req.readyState == thisObject.req.DONE) {
						handler(thisObject);
					}
				}
				this.req.send(null);
			};

		this.DELETE =
			function(url, qs, handler) {
				var thisObject = this;
				var async = (handler != null);
				this.req.open("DELETE", url + (qs ? "?"+qs : ""), async);
				if (async) this.req.onreadystatechange = function() {
					if (thisObject.req.readyState == thisObject.req.DONE) {
						handler(thisObject);
					}
				}
				this.req.send(null);
			};

		this.POST =
			function(url, qs, handler, contentType) {
				var thisObject = this;
				this.req.open("POST", url, true);
				if (handler) this.req.onreadystatechange = function() { handler(thisObject); };
				contentType = contentType ? contentType : "application/x-www-form-urlencoded";
				this.req.setRequestHeader("Content-Type", contentType);
				this.req.send(qs);
			};

		this.PUT =
			function(url, qs, handler, contentType) {
				var thisObject = this;
				this.req.open("DELETE", url, true);
				if (handler) this.req.onreadystatechange = function() { handler(thisObject); };
				contentType = contentType ? contentType : "application/x-www-form-urlencoded";
				this.req.setRequestHeader("Content-Type", contentType);
				this.req.send(qs);
			};

		this.responseText =
			function() { return this.req.responseText; };

		this.responseXML =
			function() { return this.req.responseXML; };

		this.status =
			function() { return this.req.status; };
	}

	else alert("Unable to instantiate XMLHttpRequest");
}

//*************************** Useful functions ***************************
//
var browserName = navigator.appName;
var browserVersion = navigator.appVersion;
var IE = document.all;
var CHROME = !IE && (browserVersion.indexOf("Chrome") != -1);
var FIREFOX = !IE && !CHROME && (browserName.indexOf("Netscape") != -1);
var OTHER = !IE && !CHROME && !FIREFOX;

//
//Find an object and return its size and position.
//If the object is the body, then return the visible
//dimensions rather than the total size of the page.
function findObject(obj) {
	var objPos = new Object();
	objPos.obj = obj;
	if (obj == document.body) {
		 if (window.innerWidth) {
			//standards compliant
			objPos.w = window.innerWidth;
			objPos.h = window.innerHeight;
		 }
		 else if ( (document.documentElement) &&
			 	   (document.documentElement.clientWidth) &&
			 	   (document.documentElement.clientHeight != 0) ) {
			//IE6 in standards mode
			objPos.w = document.documentElement.clientWidth;
			objPos.h = document.documentElement.clientHeight;
		 }
		 else {
		 	// older versions of IE
			objPos.w = document.body.clientWidth;
			objPos.h = document.body.clientHeight;
		}
	}
	else {
		objPos.h = obj.offsetHeight;
		objPos.w = obj.offsetWidth;
	}
	var curleft = 0;
	var curtop = 0;
	var scrollLeft = 0;
	var scrollTop = 0;
	if (obj.offsetParent) {
		curleft = obj.offsetLeft;
		curtop = obj.offsetTop;
		while (obj = obj.offsetParent) {
			curleft += obj.offsetLeft;
			curtop += obj.offsetTop;
			scrollLeft += obj.scrollLeft;
			scrollTop += obj.scrollTop;
		}
	}
	objPos.x = curleft;
	objPos.y = curtop;
	objPos.scrollLeft = scrollLeft;
	objPos.scrollTop = scrollTop;
	return objPos;
}

function getEvent(theEvent) {
	return (theEvent) ? theEvent : window.event;
}

function stopEvent(theEvent) {
	if (document.all) theEvent.cancelBubble = true;
	else theEvent.stopPropagation();
}

function getSource(theEvent) {
	return (document.all) ? theEvent.srcElement : theEvent.target;
}

function getDestination(theEvent) {
	return (document.all) ? theEvent.toElement : theEvent.relatedTarget;
}

//Get the top of an element in its parent's coordinates
function getOffsetTop(theElement) {
	return theElement.offsetTop;
}

//Get the displayed width of an object
function getObjectWidth(obj) {
	return obj.offsetWidth;
}

//Get the displayed height of an object
function getObjectHeight(obj) {
	return obj.offsetHeight;
}

//Get the vertical scroll position
function getVerticalScrollPosition() {
	var p = (document.all ? document.body.scrollTop : window.pageYOffset);
	return (p ? p : 0);
}

//Get the horizontal scroll position
function getHorizontalScrollPosition() {
	var p = (document.all ? document.body.scrollLeft : window.pageXOffset);
	return (p ? p : 0);
}

function openURL(url, target) {
	window.open(url, target);
}

function setStatusLine(text) {
	window.status = text;
	return true;
}

//Trim a string
function trim(s) {
	s = (s ? s : "");
	return s.replace(/^\s+|\s+$/g,"");
}
function ltrim(s) {
	s = (s ? s : "");
	return s.replace(/^\s+/,"");
}
function rtrim(s) {
	s = (s ? s : "");
	return s.replace(/\s+$/,"");
}
String.prototype.trim = function() {
	return this.replace(/^\s+|\s+$/g,"");
}
String.prototype.ltrim = function() {
	return this.replace(/^\s+/,"");
}
String.prototype.rtrim = function() {
	return this.replace(/\s+$/,"");
}

function listNodeTree( node, margin ) {
	if (margin == null) margin = "";
	if (node == null) return margin + "null";
	if (node.nodeType == 3) return margin + "\"" + node.nodeValue + "\"\n";
	if (node.nodeType == 1) {
		var id = node.id;
		var text = margin + node.tagName;
		var attrs = node.attributes;
		for (var k=0; k<attrs.length; k++) {
			text += "\n"+margin+"  ["+attrs[k].name+"=\""+attrs[k].value+"\"]";
		}
		text += "\n";
		for (var q=0; q<node.childNodes.length; q++) {
			text += listNodeTree( node.childNodes[q], margin + "      " );
		}
		return text;
	}
}





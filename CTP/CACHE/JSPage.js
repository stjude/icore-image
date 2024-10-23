//************************* Page Functions **************************
//
//Insert a header with a closebox and titles
function setPageHeader(title, subtitle, closeboxFile, closeboxHandler, closeboxTitle) {
	//find the first non-TextNode child of the body
	var body = document.body;
	var child = body.firstChild;
	while ((child != null) && (child.nodeType == 3)) child = child.nextSibling;

	//if the child is not a closebox div, create a header.
	if ((child.tagName != "DIV") || (child.className != "closebox")) {
		var closebox = null;
		if (closeboxFile && closeboxHandler && (closeboxFile != "")) {
			var closebox = document.createElement("DIV");
			closebox.id = "closeboxID";
			closebox.className = "closebox";
			var img = document.createElement("IMG");
			img.src = closeboxFile;
			if (closeboxTitle) img.title = closeboxTitle;
			img.onclick = closeboxHandler;
			closebox.appendChild(img);
		}
		var header = null;
		if ((title != "") || (subtitle != "")) {
			header = document.createElement("DIV");
			header.className = "header";
			if (title != "") {
				var h1 = document.createElement("H1");
				h1.appendChild(document.createTextNode(title));
				header.appendChild(h1);
			}
			if (subtitle != "") {
				var h2 = document.createElement("H2");
				h2.appendChild(document.createTextNode(subtitle));
				header.appendChild(h2);
			}
		}
		//now insert whatever isn't null
		if (header != null) body.insertBefore(header, body.firstChild);
		if (closebox != null) body.insertBefore(closebox, body.firstChild);
	}
}

function resize() {
	var bodyPos = findObject(document.body);
	var main = document.getElementById("main");
	var mainPos = findObject(main);
	var available = bodyPos.h - mainPos.y;
	var footer = document.getElementById("footer");
	if (footer) {
		var footerPos = findObject(footer);
		available -= footerPos.h;
	}
	if (available < 50) available = 50;
	main.style.height = available;
	main.style.width = bodyPos.w;
}


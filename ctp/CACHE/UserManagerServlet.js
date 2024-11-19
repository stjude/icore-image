function loaded() {
	var tools = new Array();
	tools[tools.length] = new PopupTool("/icons/save.png", "Save", null, save);
	if (home != "") tools[tools.length] = new PopupTool("/icons/home.png", "Return to the home page", home, null);
	setPopupToolPanel( tools );

	showHideColumns();
}
window.onload = loaded;

function save() {
	var form = document.getElementById("formID");
	form.target = "_self";
	form.submit();
}

function toggleRoles(n, event) {
	var evt = getEvent(event);
	var cb = getSource(evt);
	var sel = cb.checked;
	var table = document.getElementById("userTable");
	var tbody = table.tBodies[0];
	var rows = tbody.getElementsByTagName("TR");
	for (var i=0; i<rows.length; i++) {
		var cbs = rows[i].getElementsByTagName("INPUT");
		cbs[n+1].checked = sel;
	}
}

function showHideColumns() {
		var button = document.getElementById("shRoles");
		var show = (button.value.indexOf("Show") != -1);
		var table = document.getElementById("userTable");
		var thead = table.tHead;
		var tbody = table.tBodies[0];
		if (show) {
			var cells = table.getElementsByTagName("TH");
			for (var i=0; i<cells.length; i++) cells[i].style.display = "table-cell";
			var cells = tbody.getElementsByTagName("TD");
			for (var i=0; i<cells.length; i++) cells[i].style.display = "table-cell";
			button.value = "Hide Unused Roles";
		}
		else {
			var rows = tbody.getElementsByTagName("TR");
			//see what roles are in use
			var len = rows[0].getElementsByTagName("TD").length;
			var used = new Array();
			for (var i=0; i<len; i++) used[i] = false;

			for (var k=0; k<rows.length; k++) {
				var tds = rows[k].getElementsByTagName("TD");
				for (var kk=0; kk<tds.length; kk++) {
					var inp = tds[kk].firstChild;
					if ((inp != null) && ((inp.type != "checkbox") || inp.checked)) used[kk] = true;
				}
			}

			//Now turn off the columns that aren't in use
			hideColumns(thead, "TH", used);
			hideColumns(tbody, "TD", used);
			button.value = "Show All Roles"
		}
}

function hideColumns(part, name, used) {
	var rows = part.getElementsByTagName("TR");
	for (var i=0; i<rows.length; i++) {
		var cells = rows[i].getElementsByTagName(name);
		for (var j=0; j<cells.length; j++) {
			if (!used[j]) cells[j].style.display = "none";
		}
	}
}

function showRolesPopup() {
	var id = "rolesPopupID";
	var pop = document.getElementById(id);
	if (pop) pop.parentNode.removeChild(pop);

	var div = document.createElement("DIV");
	div.className = "content";
	var w = 400;
	var h = 400;
	var iframe = document.createElement("IFRAME");
	iframe.style.width = w - 30;
	iframe.style.height = h - 55;
	iframe.src = "/UserManagerRoles.html";
	div.appendChild(iframe);
	var closebox = "/icons/closebox.gif";
	showDialog(id, w, h, "Standard Role Definitions", closebox, null, div, null, null);
}

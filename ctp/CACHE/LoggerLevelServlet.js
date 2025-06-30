function loaded() {
	document.getElementById("ClassSelector").focus();
}
window.onload = loaded;

function save() {
	var form = document.getElementById("formID");
	form.target = "_self";
	form.submit();
}

function setClass() {
	var classfield = document.getElementById("class");
	var select = document.getElementById("ClassSelector");
	if (select) {
		var index = select.selectedIndex;
		classfield.value = select.options[index].value;
	}
}
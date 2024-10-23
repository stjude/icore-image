//A class to synchronize the widths of two tables.
//This class can be used to implement the equivalent
//of a single table with a non-scrolling header and
//a scrollable body.
//
//Variables:
//	head - the table containing a THEAD element with the column headings
//  body - the table containing a TBODY element whose column widths are
//			to be forced into the head table.
function ScrollableTable( head, body ) {
	this.headTable = head;
	this.bodyTable = body;
	this.thead = head.tHead;
	this.tbody = body.tBodies[0];
	this.blanks = "\u00A0\u00A0"; //two blanks
}

ScrollableTable.prototype.sync = function() {
	this.headTable.style.width = this.bodyTable.clientWidth;
	var headRows = this.thead.rows;
	var bodyRows = this.tbody.rows;
	if ((headRows.length > 0) && (bodyRows.length > 0)) {
		var bodyRowCells = bodyRows[0].cells;
		for (var i=0; i < headRows.length; i++) {
			var headRowCells = headRows[i].cells;
			for (var k=0; (k<headRowCells.length) && (k<bodyRowCells.length); k++) {
				var hc = headRowCells[k];
				var bc = bodyRowCells[k];
				var style = (bc.currentStyle ? bc.currentStyle : bc.style);
				var pad = parseInt(style.paddingLeft) + parseInt(style.paddingRight);
				if (isNaN(pad)) pad = 4;
				var w = bc.clientWidth - pad;
				hc.width = ( (w>1) ? w : 1);
			}
		}
	}
}

ScrollableTable.prototype.rationalize = function() {
	var headCells = this.thead.rows[0].cells;
	var bodyCells = this.tbody.rows[0].cells;
	var headLengths = getCellLengths(headCells);
	var bodyLengths = getCellLengths(bodyCells);

	for (var i=0; i<headCells.length; i++) {
		var h = headCells[i];
		var b = bodyCells[i];

		var dif = headLengths[i] - bodyLengths[i];
		if (dif > -2) {
			var ftn = getLastTextNode(b);
			if (ftn != null) {
				var text = "";
				if (dif < 3) dif = 3;
				for (var k=0; k<dif; k++) text += this.blanks;
				text = ftn.nodeValue + text;
				var tn = document.createTextNode(text);
				ftn.parentNode.replaceChild(tn, ftn);
			}
		}
	}

	function getCellLengths(cells) {
		var lengths = new Array();
		for (var i=0; i<cells.length; i++) {
			lengths[i] = getTextLength(cells[i]);
		}
		return lengths;
	}

	function getTextLength(node) {
		var len = 0;
		var children = node.childNodes;
		for (var i=0; i<children.length; i++) {
			var c = children[i];
			if (c.nodeType == 3) len += c.nodeValue.length;
			else if (c.nodeType == 1) len += getTextLength(c);
		}
		return len;
	}

	function getLastTextNode(node) {
		var children = node.childNodes;
		for (var i=children.length-1; i>=0; i--) {
			var c = children[i];
			if (c.nodeType == 3) return c;
			else if (c.nodeType == 1) {
				var n = getLastTextNode(c);
				if (n != null) return n;
			}
		}
		return null;
	}

}


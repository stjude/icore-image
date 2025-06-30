//-----------------------------------------------------------------------------
// sortTable(id, col, rev)
//
//  id  - ID of the TABLE, TBODY, THEAD or TFOOT element to be sorted.
//  col - Index of the column to sort, 0 = first column, 1 = second column,
//        etc.
//  rev - If true, the column is sorted in reverse (descending) order
//        initially.
//  highlight - If true, apply styles to the rows and columns
//-----------------------------------------------------------------------------

function sortTable(id, col, rev, forceTextSort, highlight) {

  // Get the table or table section to sort.
  var tblEl = document.getElementById(id);

  // The first time this function is called for a given table, set up an
  // array of reverse sort flags.
  if (tblEl.reverseSort == null) {
    tblEl.reverseSort = new Array();
    tblEl.lastColumn = 0;
  }

  // If this column has not been sorted before, set the initial sort direction.
  if (tblEl.reverseSort[col] == null)
    tblEl.reverseSort[col] = rev;

  // If this column was the last one sorted, reverse its sort direction.
  if (col == tblEl.lastColumn)
    tblEl.reverseSort[col] = !tblEl.reverseSort[col];

  // Remember this column as the last one sorted.
  tblEl.lastColumn = col;

  // Set the table display style to "none" - necessary for Netscape 6
  // browsers.
  var oldDsply = tblEl.style.display;
  tblEl.style.display = "none";

  // Sort the rows based on the content of the specified column using a
  // selection sort.

  var tmpEl;
  var i, j;
  var minVal, minIdx;
  var testVal;
  var cmp;

  for (i = 0; i < tblEl.rows.length - 1; i++) {

    // Assume the current row has the minimum value.
    minIdx = i;
    minVal = sortTable_getTextValue(tblEl.rows[i].cells[col]);

    // Search the rows that follow the current one for a smaller value.
    for (j = i + 1; j < tblEl.rows.length; j++) {
      testVal = sortTable_getTextValue(tblEl.rows[j].cells[col]);
      cmp = sortTable_compareValues(minVal, testVal, forceTextSort);
      // Negate the comparison result if the reverse sort flag is set.
      if (tblEl.reverseSort[col])
        cmp = -cmp;
      // If this row has a smaller value than the current minimum, remember its
      // position and update the current minimum value.
      if (cmp > 0) {
        minIdx = j;
        minVal = testVal;
      }
    }

    // By now, we have the row with the smallest value. Remove it from the
    // table and insert it before the current row.
    if (minIdx > i) {
      tmpEl = tblEl.removeChild(tblEl.rows[minIdx]);
      tblEl.insertBefore(tmpEl, tblEl.rows[i]);
    }
  }

  // Make it look pretty.
  if (highlight) sortTable_alternateRows(tblEl, col);
  sortTable_highlightSortColumn(tblEl, col);

  // Restore the table's display style.
  tblEl.style.display = oldDsply;

  return false;
}

//-----------------------------------------------------------------------------
// Functions to get and compare values during a sort.
//-----------------------------------------------------------------------------

// This code is necessary for browsers that don't reflect the DOM constants
if (document.ELEMENT_NODE == null) {
  document.ELEMENT_NODE = 1;
  document.TEXT_NODE = 3;
}

function sortTable_getTextValue(el) {

  var i;
  var s;

  // Find and concatenate the values of all text nodes contained within the
  // element.
  s = "";
  for (i = 0; i < el.childNodes.length; i++)
    if (el.childNodes[i].nodeType == document.TEXT_NODE)
      s += el.childNodes[i].nodeValue;
    else if (el.childNodes[i].nodeType == document.ELEMENT_NODE &&
             el.childNodes[i].tagName == "BR")
      s += " ";
    else
      // Use recursion to get text within sub-elements.
      s += sortTable_getTextValue(el.childNodes[i]);

  return sortTable_normalizeString(s);
}

function sortTable_compareValues(v1, v2, forceTextSort) {
  if (!forceTextSort) {
    // If the values are numeric, convert them to floats.
    var f1 = parseFloat(v1);
    var f2 = parseFloat(v2);
    if (!isNaN(f1) && !isNaN(f2)) {
      v1 = f1;
      v2 = f2;
    }
  }
  // Compare the two values.
  if (v1 == v2) return 0;
  if (v1 > v2) return 1
  return -1;
}

// Regular expressions for normalizing white space.
var whtSpEnds = new RegExp("^\\s*|\\s*$", "g");
var whtSpMult = new RegExp("\\s\\s+", "g");

function sortTable_normalizeString(s) {

  s = s.replace(whtSpMult, " ");  // Collapse multiple whitespace.
  s = s.replace(whtSpEnds, "");   // Remove leading and trailing white space.

  return s;
}

//-----------------------------------------------------------------------------
// Functions to update the table appearance after a sort.
//-----------------------------------------------------------------------------

// Style class names.
var rowClsNm = "alternateRow";
var colClsNm = "sortedColumn";

// Regular expressions for setting class names.
var rowTest = new RegExp(rowClsNm, "gi");
var colTest = new RegExp(colClsNm, "gi");

function sortTable_alternateRows(tblEl, col) {
  var i, j;
  var rowEl, cellEl;
  // Set style classes on each row to alternate their appearance.
  for (i = 0; i < tblEl.rows.length; i++) {
   rowEl = tblEl.rows[i];
   rowEl.className = rowEl.className.replace(rowTest, "");
    if (i % 2 != 0)
      rowEl.className += " " + rowClsNm;
    rowEl.className = sortTable_normalizeString(rowEl.className);
    // Set style classes on each column to
    // highlight the one that was sorted.
    for (j = 0; j < tblEl.rows[i].cells.length; j++) {
      cellEl = rowEl.cells[j];
      cellEl.className = cellEl.className.replace(colTest, "");
      if (j == col) cellEl.className += " " + colClsNm;
      cellEl.className = sortTable_normalizeString(cellEl.className);
    }
  }
}

function sortTable_highlightSortColumn(tblEl, col) {
  var i, j;
  var rowEl, cellEl;
  // Find the table header and highlight the column that was sorted.
  var el = tblEl.parentNode.tHead;
  rowEl = el.rows[el.rows.length - 1];
  // Set style classes for each column.
  for (i = 0; i < rowEl.cells.length; i++) {
    cellEl = rowEl.cells[i];
    cellEl.className = cellEl.className.replace(colTest, "");
    // Highlight the header of the sorted column.
    if (i == col) cellEl.className += " " + colClsNm;
    cellEl.className = sortTable_normalizeString(cellEl.className);
  }
}

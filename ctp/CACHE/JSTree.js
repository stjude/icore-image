//Manager for an array of Trees.
//id: the id of a div in which to display the Trees
//url: the URL from which to obtain the XML definition of the Trees
//plus: the URL of the plus-sign icon for Tree expansion
//minus: the URL of the minus-sign icon for Tree collapse
function TreeManager(id, url, plus, minus) {
	this.id = id;
	this.url = url;
	this.plus = plus;
	this.minus = minus;
	this.trees = new Array();
	this.div = document.getElementById(this.id);
}

//Load all the Trees from the XML definition obtained from the url property.
TreeManager.prototype.load = function(qs) {
	var req = new AJAX();
	qs = (qs ? qs + "&" : "") + req.timeStamp();
	req.GET(this.url, qs, null);
	if (req.success()) {
		this.trees = new Array();
		var xml = req.responseXML();
		var root = xml.firstChild;
		if (this.div) {
			var child = root.firstChild;
			while (child) {
				if (child.nodeType == 1) {
					var tree = new Tree(this, null, 0);
					tree.load(child);
					this.trees[this.trees.length] = tree;
				}
				child = child.nextSibling;
			}
		}
		else alert("Unable to find the tree div");
	}
	else alert("Unable to obtain the tree");
}

//Close all the paths for all the Trees.
TreeManager.prototype.closePaths = function() {
	//hide all paths
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].closePath();
	}
}

//Display all the Trees in the div identified by the id property.
//If the state parameter is non-null, apply the state to the Trees.
TreeManager.prototype.display = function(state) {
	//empty the div
	while (this.div.firstChild) this.div.removeChild(this.div.firstChild);
	//load all the branches
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].display();
	}
	if (state == null) {
		//show the top level only
		for (var i=0; i<this.trees.length; i++) {
			this.trees[i].collapse();
		}
	}
	else this.setStates(state);
}

//Get an array of state objects indicating
//which Trees are expanded.
TreeManager.prototype.getState = function() {
	var state = new Array();
	for (var i=0; i<this.trees.length; i++) {
		state[i] = this.trees[i].getState();
	}
	return state;
}

//Set the states of the Trees from a supplied object.
//This object is an array of state objects, each of
//which refers to a Tree. Trees are identified
//by their name property.
TreeManager.prototype.setStates = function(states) {
	for (var i=0; i<states.length; i++) {
		var tree = this.getTreeForName(states[i].name);
		if (tree != null) tree.setState(states[i]);
	}
}

//Get a Tree object with a specific name
//from the TreeManager's trees array.
TreeManager.prototype.getTreeForName = function(name) {
	for (var i=0; i<this.trees.length; i++) {
		if (this.trees[i].name == name) return this.trees[i];
	}
	return null;
}

//Get a Tree object with a specific name
//from the TreeManager's trees array.
TreeManager.prototype.getTreeForName = function(name) {
	for (var i=0; i<this.trees.length; i++) {
		if (this.trees[i].name == name) return this.trees[i];
	}
	return null;
}

//Get a Tree object which lies under a set of coordinates
TreeManager.prototype.getTreeForCoords = function(x, y) {
	for (var i=0; i<this.trees.length; i++) {
		var tree = this.trees[i].getTreeForCoords(x, y);
		if (tree != null) return tree;
	}
	return null;
}

//Expand all the Trees included on a specified path.
TreeManager.prototype.expandPath = function(path) {
	var lastTreeExpanded = null;
	var pathElements = path.split(/\//);
	if (pathElements.length > 0) {
		var tree = this.getTreeForName(pathElements[0]);
		if (tree != null) {
			tree.expand();
			lastTreeExpanded = tree;
			for (var i=1; i<pathElements.length; i++) {
				tree = tree.getTreeForName(pathElements[i]);
				if (tree != null) {
					tree.expand();
					lastTreeExpanded = tree;
				}
				else break;
			}
		}
	}
	return lastTreeExpanded;
}

//Expand all Trees.
TreeManager.prototype.expandAll = function() {
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].expandAll();
	}
}

//Create a Tree from an XML element
//
//Notes about Trees:
//-- A Tree consists of a name and an array of child Trees.
//-- If a Tree has child Trees, the Tree can be expanded or
//   collapsed.
//-- If collapsed, the Tree name is shown and a plus-sign
//   icon is displayed, but no child Trees are displayed.
//-- If expanded, the Tree name is shown, a minus-sign
//   icon is displayed, and all the child Trees are displayed.
//-- A path is a string listing all the names from the root
//   to the node.
//-- When a path is shown, it is indicated by applying the
//   CSS class "open" to the names of all the path
//   elements back to the root of the Tree.
//-- When a path is hidden, it is indicated by applying the
//   CSS class "closed" to the names of all the path
//   elements back to the root of the Tree.
//-- Showing or hiding a path does not change whether the path
//   is displayed, only how it is displayed. The purpose is to
//   make it easy for the user to see the path in the Tree that
//   is currently open (as in the case of a directory being
//   selected in the left pane and the contents being shown in
//   the right pane.
//
function Tree(treeManager, parent, level) {
	this.indent = 15;
	this.treeManager = treeManager;
	this.parent = parent;
	this.level = level;
	this.trees = new Array();
	this.expanded = false;
}

Tree.prototype.load = function(node) {
	this.name = node.getAttribute("name");

	this.sclickHandler = this.getAttrValue(node,"sclickHandler");
	this.sclickURL = this.getAttrValue(node,"sclickURL");

	this.dclickHandler = this.getAttrValue(node,"dclickHandler");
	this.dclickURL = this.getAttrValue(node,"dclickURL");

	this.dropHandler = this.getAttrValue(node,"dropHandler");
	this.dropURL = this.getAttrValue(node,"dropURL");

	this.nodeID = this.getAttrValue(node,"nodeID");

	var child = node.firstChild;
	while (child) {
		if (child.nodeType == 1) {
			var tree = new Tree(this.treeManager, this, this.level+1);
			this.trees[this.trees.length] = tree;
			tree.load(child);
		}
		child = child.nextSibling;
	}
}

//Create an empty Tree and append it to the array of Trees for this Tree.
Tree.prototype.appendTree = function(name,
									 sclickHandler, sclickURL,
									 dclickHandler, dclickURL,
									 dropHandler, dropURL) {
	var tree = new Tree(this.treeManager, this, this.level + 1);
	tree.name = name;

	tree.sclickHandler = sclickHandler;
	tree.sclickURL = sclickURL;

	tree.dclickHandler = dclickHandler;
	tree.dclickURL = dclickURL;

	tree.dropHandler = dropHandler;
	tree.dropURL = dropURL;

	this.trees[this.trees.length] = tree;
}

//Replace an old Tree with a new one.
//If the old Tree is not in the list,
//then append the new one to the list.
Tree.prototype.replaceTree = function(oldTree, newTree) {
	for (var i=0; i<this.trees.length; i++) {
		if (this.trees[i] === oldTree) {
			this.trees[i] = newTree;
			return newTree;
		}
	}
	this.trees[this.trees.length] = newTree;
	return newTree;
}

//Replace the children of this tree with
//the children of another tree.
Tree.prototype.replaceChildren = function(newTree) {
	this.trees = new Array();
	for (var i=0; i<newTree.trees.length; i++) {
		var child = newTree.trees[i];
		child.parent = this;
		this.trees[this.trees.length] = child;
	}
	return this;
}

//Get the state object for this Tree.
Tree.prototype.getState = function() {
	var state = new Object();
	state.name = this.name;
	state.expanded = this.expanded;
	state.trees = new Array();
	for (var i=0; i<this.trees.length; i++) {
		state.trees[i] = this.trees[i].getState();
	}
	return state;
}

//Set the state of the Tree from a supplied object.
//This object is a state object.
Tree.prototype.setState = function(state) {
	if (state.expanded) {
		this.expand();
		for (var i=0; i<state.trees.length; i++) {
			var tree = this.getTreeForName(state.trees[i].name);
			if (tree != null) tree.setState(state.trees[i]);
		}
	}
	else this.collapse();
}

//Get a Tree object with a specific name
//from this Tree's trees array.
Tree.prototype.getTreeForName = function(name) {
	for (var i=0; i<this.trees.length; i++) {
		if (this.trees[i].name == name) return this.trees[i];
	}
	return null;
}

//Get a Tree object which contains a coordinate pair
Tree.prototype.getTreeForCoords = function(x, y) {
	var pos = findObject(this.div);
//	alert("checking "+this.name+"\n\nx="+pos.x+"; y="+pos.y+"; w="+pos.w+"; h="+pos.h+"\n\nx="+x+"; y="+y);
	if (pos &&
		(pos.x <= x) && (pos.x + pos.w > x) &&
		(pos.y <= y) && (pos.y + pos.h > y)) return this;
	for (var i=0; i<this.trees.length; i++) {
		var tree = this.trees[i].getTreeForCoords(x, y);
		if (tree != null) return tree;
	}
	return null;
}

//Get a string containing the complete path to this
//node, with path elements separated by slashes.
Tree.prototype.getPath = function() {
	var node = this;
	var path = node.name;
	while (node.parent != null) {
		node = node.parent;
		path = node.name + "/" + path;
	}
	return path;
}

//Show as being open the path from this
//node up to the root of the Tree,
Tree.prototype.showPath = function() {
	var node = this;
	while (node != null) {
		node.div.className = "open";
		node = node.parent;
	}
}

//Close all paths from this tree down.
Tree.prototype.closePath = function() {
	this.div.className = "closed";
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].closePath();
	}
}

//Get the value of an attribute of an XML node.
//Trim the value of the attribute. If the result
//is the empty string, return null.
Tree.prototype.getAttrValue = function(node, attr) {
	var value = node.getAttribute(attr);
	if (value != null) {
		value = value.replace(/^\s*/, "").replace(/\s*$/, "");
		if (value == "") value = null;
	}
	return value;
}

//Display this Tree.
Tree.prototype.display = function() {
	this.div = document.createElement("DIV");
	this.div.className = "closed";
	this.div.tree = this;
	this.expanded = false;
	var toggle = null;
	if (this.trees.length != 0) {
		toggle = document.createElement("IMG");
		toggle.setAttribute("src", this.treeManager.minus);
		toggle.onclick = collapseTree;
		this.div.appendChild(toggle);
		this.div.style.marginLeft = this.level * this.indent;
	}
	else {
		this.div.style.marginLeft = this.level * this.indent + 17;
	}
	var span = document.createElement("SPAN");
	span.style.cursor = "pointer";
	this.namespan = span;

	if (this.sclickHandler) {
		span.onmouseenter = highlightTreeNode;
		span.onmouseleave = dehighlightTreeNode;
		span.onclick = eval(this.sclickHandler);
	}
	if (this.dclickHandler) {
		span.onmouseenter = highlightTreeNode;
		span.onmouseleave = dehighlightTreeNode;
		span.ondblclick = eval(this.dclickHandler);
	}

	span.treenode = this;

	span.appendChild(document.createTextNode(this.name));
	this.div.appendChild(span);
	this.treeManager.div.appendChild(this.div);
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].display();
	}

	function highlightTreeNode() { span.className = "highlightTreeNode"; }
	function dehighlightTreeNode() { span.className = "dehighlightTreeNode"; }
}

//Collapse this Tree so that the Tree name is visible
//but none of its child Trees are visible.
Tree.prototype.collapse = function() {
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].hideAll();
	}
	this.show();
	this.expanded = false;
	if (this.trees.length != 0) {
		var img = this.div.firstChild;
		img.src = this.treeManager.plus;
		img.onclick = expandTree;
	}
}

//Hide all the divs for this Tree and its child Trees.
Tree.prototype.hideAll = function() {
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].hideAll();
	}
	this.hide();
}

//Hide the div for this Tree.
Tree.prototype.hide = function() {
	this.div.style.visibility = "hidden";
	this.div.style.display = "none";
}

//Expand this Tree so that its child Trees are visible.
Tree.prototype.expand = function() {
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].showName();
	}
	this.show();
	this.expanded = true;
	if (this.trees.length != 0) {
		var img = this.div.firstChild;
		img.src = this.treeManager.minus;
		img.onclick = collapseTree;
	}
}

//Expand this Tree and all its child Trees
Tree.prototype.expandAll = function() {
	this.expand();
	for (var i=0; i<this.trees.length; i++) {
		this.trees[i].expandAll();
	}
}

//Show the name of this Tree
Tree.prototype.showName = function() {
	this.show();
	if (this.trees.length != 0) {
		var img = this.div.firstChild;
		img.src = this.treeManager.plus;
		img.onclick = expandTree;
	}
}

//Make the div for this Tree visible.
Tree.prototype.show = function() {
	this.div.style.visibility = "visible";
	this.div.style.display = "block";
}

function collapseTree(event) {
	var evt = getEvent(event);
	var src = getSource(evt);
	var parent = src.parentNode;
	parent.tree.collapse();
}

function expandTree(event) {
	var evt = getEvent(event);
	var src = getSource(evt);
	var parent = src.parentNode;
	parent.tree.expand();
}

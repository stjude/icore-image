//Menu Package

//some global parameters
var menuZIndexMax = 30;
var menuZIndexMin = 20;
var inMenu = false;
var menuTimeout = 200;
var menuTimeoutId = null;
var menuTimerRunning = false;

//****** The MenuBar class ******
//
function MenuBar(id, menus, text) {
	this.menus = menus;
	this.id = id; 		//the id of the MenuBar div

	//set up a description object for the right side of the menubar
	this.desc = document.createElement("DIV");
	this.desc.className = "desc";
	this.text = text ? text : ""; //the text for the description object;

	inMenu = false;
	this.setPointers(); //set the pointers so everybody knows who's who.
}

MenuBar.prototype.setPointers = function() {
	//Note: the index is an object containing properties
	//for all Items that have name properties. This is
	//intended to allow a program to find a specific
	//Item without having to know where it has been placed
	//in the menu system.
	//
	//This function builds the index and also sets up
	//the pointers so that all items know who their
	//parents are.
	//
	//This function must be called immediately after
	//a change is made to the menuBar or any of its
	//menus. Failure to do so may result in handlers
	//not being able to navigate their menus.
	this.index = new Object();
	for (var i=0; i<this.menus.length; i++) {
		this.menus[i].setPointers(this.index, this.menus[i]);
	}
}

MenuBar.prototype.setEnable = function(name, enb) {
	//Enable the named object.
	var object = this.index[name];
	if (object) object.enabled = enb;
}

MenuBar.prototype.display = function() {
	var div = document.getElementById(this.id);
	if (div) {
		div.className = "MenuBar";
		div.isMenuSystemDiv = true;
		div.onmouseout = checkHideMenus;
		div.onmousein = setInMenu;
		//remove all the children
		while (div.firstChild) div.removeChild(div.firstChild);

		//put in the description element at the right side of the menubar.
		div.appendChild(this.desc);
		this.setText(this.text);

		//put in the menu titles
		var hasMenu = false;
		for (var i=0; i<this.menus.length; i++) {
			if (this.menus[i].enabled) {
				if (hasMenu) div.appendChild(document.createTextNode("\u00A0\u00A0\u00A0|\u00A0\u00A0\u00A0"));
				var s = document.createElement("SPAN");
				s.appendChild(document.createTextNode(this.menus[i].title));
				s.onmouseover = highlight;
				s.onmouseout = dehighlight;
				s.onmouseenter = menuTitleClicked; //autodisplay
				s.onclick = menuTitleClicked;
				s.item = this.menus[i];
				div.appendChild(s);
				hasMenu = true;
			}
		}
	}
}

MenuBar.prototype.setText = function(text) {
	this.text = text;
	while (this.desc.firstChild) this.desc.removeChild(this.desc.firstChild);
	this.desc.appendChild(document.createTextNode(text));
}
//****** End of the MenuBar class ******

//****** The SingleMenuBar class ******
//
//This class is a simplified MenuBar that manages a single menu.
//It is intended for situations where multiple pull-down menus
//appear in different places on the page. Each menu must have
//its own SingleMenu instance to manage it. (Note: as in the
//case of the MenuBar class, Menu objects can contain submenus.)
//
//Note: In this class, the constructor argument is not an
//array of Menu objects as it is in the MenuBar class; it
//is just the single Menu object.
//
//Note: The underlying DIV for the SingleMenuBar should
//have the display:inline style in order to have the proper
//behavior when the cursor is moved out of the menu title.
//(It is possible to use a SPAN instead, but then the pull-
//down menu doesn't collapse unless the cursor is moved into
//the menu and then out. A word to the wise.)
function SingleMenuBar(id, menu) {
	this.menu = menu;
	this.id = id;

	inMenu = false;
	this.setPointers();
}

SingleMenuBar.prototype.setPointers = function() {
	this.index = new Object();
	this.menu.setPointers(this.index, this.menu);
}

SingleMenuBar.prototype.setEnable = function(name, enb) {
	var object = this.index[name];
	if (object) object.enabled = enb;
}

SingleMenuBar.prototype.display = function() {
	var div = document.getElementById(this.id);
	if (div) {
		div.className = "MenuBar";
		div.isMenuSystemDiv = true;
		div.onmouseout = checkHideMenus;
		div.onmousein = setInMenu;
		while (div.firstChild) div.removeChild(div.firstChild);

		if (this.menu.enabled) {
			var s = document.createElement("SPAN");
			s.appendChild(document.createTextNode(this.menu.title));
			//s.onmouseover = highlight;
			//s.onmouseout = dehighlight;
			//s.onmouseenter = menuTitleClicked; //no autodisplay on single menus
			s.onclick = menuTitleClicked;
			s.item = this.menu;
			div.appendChild(s);
		}
	}
}
//****** End of the SingleMenuBar class ******

//****** The Menu class ******
function Menu(title, items, name) {
	this.title = title;
	this.items = items;
	this.name = name;
	this.enabled = true;
}

Menu.prototype.setPointers = function(index, parentMenu) {
	if ((this.name != null) && (this.name != "")) index[this.name] = this;
	this.parentMenu = parentMenu;
	for (var i=0; i<this.items.length; i++) {
		this.items[i].setPointers(index, this);
	}
}

Menu.prototype.replaceItem = function(newItem, oldItem) {
	for (var i=0; i<this.items.length; i++) {
		if (this.items[i] == oldItem) {
			this.items[i] = newItem;
			return;
		}
	}
}

Menu.prototype.display = function(x, y, submenu, zIndex) {
	this.zIndex = zIndex;
	var div = document.createElement("DIV");
	div.className = "Menu";
	if (submenu) div.style.paddingLeft = 0;
	div.isMenuSystemDiv = true;
	div.onmouseout = checkHideMenus;
	div.style.left = x;
	div.style.top = y;
	div.style.zIndex = zIndex;
	var firstItem = true;
	for (var i=0; i<this.items.length; i++) {
		var item = this.items[i];
		if (item.enabled) {
			if ((item.title == null) || (item.title == "")) {
				var hr = document.createElement("HR");
				if (submenu) hr.style.marginLeft = 15;
				div.appendChild(hr);
			}
			else {
				if (submenu) {
					var s = document.createElement("SPAN");
					s.className = "larr";
					s.appendChild(document.createTextNode( (firstItem ? "\u00AB" : "") ));
					div.appendChild(s);
				}
				var s = document.createElement("SPAN");
				s.appendChild(document.createTextNode(item.title));
				if ((item instanceof Menu) || (item.handler)) {
					s.onmouseover = highlight;
					s.onmouseout = dehighlight;
					s.onclick = menuItemClicked;
					s.item = item;
				}
				else s.className = "unselectable";
				if (item instanceof Menu) {
					s.onmouseenter = menuItemClicked; //autodisplay
				}
				else s.onmouseenter = removeLowerMenus;
				div.appendChild(s);
				div.appendChild(document.createElement("BR"));
			}
			firstItem = false;
		}
		else {
			var s = document.createElement("SPAN");
			s.appendChild(document.createTextNode(item.title));
			s.className = "notenabled";
			div.appendChild(s);
			div.appendChild(document.createElement("BR"));
			firstItem = false;
		}
	}
	removeZRange(menuZIndexMin, zIndex);
	document.body.appendChild(div);
	inMenu = true;
}
//****** End of the Menu class ******

//****** The Item class ******
function Item(title, handler, name) {
	this.title = title;
	this.handler = handler;
	this.name = name;
	this.enabled = true;
}

Item.prototype.setPointers = function(index, parentMenu) {
	if ((this.name != null) && (this.name != "")) index[this.name] = this;
	this.parentMenu = parentMenu;
}
//****** End of the Item class ******

//****************** Important handlers *******************

//This handler catches the timeout for hiding the menus
//when the mouse has moved to a non-menu part of the
//screen.
function menuTimeoutHandler(event) {
	stopMenuTimer();
	if (!inMenu) removeMenus();
}

//This handler catches the onmousein event and sets
//the flag indicating that the cursor is over a menu
function setInMenu(event) {
	event = getEvent(event);
	var source = getSource(event);
	var sourceDiv = source;
	while ((sourceDiv != null) && (sourceDiv.tagName != "DIV")) sourceDiv = sourceDiv.parentNode;
	if (sourceDiv != null) inMenu = sourceDiv.isMenuSystemDiv;
	else alert("sourceDiv is null");
}

//This handler catches the click on a menu title
//in the MenuBar. It gets the MenuBar and the Menu
//from the menu title and calls the MenuBar to
//display the menu.
function menuTitleClicked(event) {
	event = getEvent(event);
	var source = getSource(event);
	var menu = source.item.parentMenu;
	var pos = findObject(source);
	removeMenus();

	var x = pos.x - 15 - pos.scrollLeft;
	var y = pos.y + pos.h + (document.all ? 3 : 4) - pos.scrollTop;
	menu.display(x, y, false, menuZIndexMax);
}

//This handler catches the click on a menu item
//in a Menu. It gets the Item and handles it.
//
//If the handler specified for the item is null,
//it does nothing.
//
//If the handler is a menu, it displays the
//menu as a submenu, and leaves all higher-level
//menus in place.
//
//Otherwise, it hides all the menus and then
//calls the specified handler, passing the
//event and the item.
function menuItemClicked(event) {
	event = getEvent(event);
	var source = getSource(event);
	var item = source.item;
	if (item instanceof Menu) {
		var srcpos = findObject(source);
		var sourceDiv = source;
		while ((sourceDiv != null) && (sourceDiv.tagName != "DIV")) sourceDiv = sourceDiv.parentNode;
		var divpos = findObject(sourceDiv);
		var menu = source.item.parentMenu;
		var x = divpos.x + divpos.w -1;
		var y = srcpos.y -4;
		var z = item.parentMenu.zIndex - 1;
		item.display(x, y, true, z);
	}
	else {
		removeMenus();
		if (item.handler != null) item.handler(event, item);
	}
}

//Handle the onmouseout for a MenuBar div or a Menu div,
//and determine whether it is necessary to hide the menus.
function checkHideMenus(event) {
	event = getEvent(event);
	var source = getSource(event);
	var dest = getDestination(event);

	//Find the source DIV.
	var sourceDiv = source;
	while ((sourceDiv != null) && (sourceDiv.tagName != "DIV")) sourceDiv = sourceDiv.parentNode;
	var sourceIsMenuSystemDiv = (sourceDiv != null) && sourceDiv.isMenuSystemDiv;

	//Find the destination DIV
	var destDiv = dest;
	while ((destDiv != null) && (destDiv.tagName != "DIV")) destDiv = destDiv.parentNode;
	var destIsMenuSystemDiv = (destDiv != null) && destDiv.isMenuSystemDiv;

	//See if we are leaving the menu system
	if (sourceIsMenuSystemDiv && !destIsMenuSystemDiv) {
		startMenuTimer();
		inMenu = false;
	}
	else inMenu = true;
}

//This handler catches the onmouseover event
//for menu titles and items and sets the
//className to highlight them.
function highlight(event) {
	event = getEvent(event);
	var source = getSource(event);
	source.className = "highlight";
}

//This handler catches the onmouseout event
//for menu titles and items and sets the
//className to dehighlight them.
function dehighlight(event) {
	event = getEvent(event);
	var source = getSource(event);
	source.className = "dehighlight";
}

//This handler catches the onmouseenter event
//for non-menu items and removes any lower-level
//menus.
function removeLowerMenus(event) {
	event = getEvent(event);
	var source = getSource(event);
	var z = source.parentNode.style.zIndex;
	removeZRange(menuZIndexMin, z - 1);
}

//*************************** Timer functions ***************************

function startMenuTimer() {
	stopMenuTimer();
	menuTimeoutId = window.setTimeout(menuTimeoutHandler, menuTimeout);
	menuTimerRunning = true;
}

function stopMenuTimer() {
	if (menuTimerRunning) {
		window.clearTimeout(menuTimeoutId);
		menuTimerRunning = false;
	}
}

//*************************** Useful functions ***************************

function removeMenus() {
	inMenu = false;
	stopMenuTimer();
	removeZRange(menuZIndexMin, menuZIndexMax);
}

function removeZRange(lowerZIndex, upperZIndex) {
	var divs = document.getElementsByTagName("DIV");
	for (var i=divs.length-1; i>=0; i--) {
		var z = divs[i].style.zIndex;
		if ((z >= lowerZIndex) && (z <= upperZIndex)) {
			divs[i].parentNode.removeChild(divs[i]);
		}
	}
}

function hideZRange(lowerZIndex, upperZIndex) {
	var divs = document.getElementsByTagName("DIV");
	for (var i=divs.length-1; i>=0; i--) {
		var z = divs[i].style.zIndex;
		if ((z >= lowerZIndex) && (z <= upperZIndex)) {
			divs[i].style.visibility = "hidden";
			divs[i].style.display = "none";
		}
	}
}

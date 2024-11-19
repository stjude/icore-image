function HorizontalSplit(leftDiv, sliderDiv, rightDiv, fillHeight,
							sliderPosition, forceTopForIE,
							leftMin, rightMin,
							changeHandler) {
	this.sliderWidth = 6;
	this.lmin = (leftMin ? leftMin : 120);
	this.rmin = (rightMin ? rightMin : 120);
	this.leftWidth = (sliderPosition ? sliderPosition : 300);

	this.fillHeight = fillHeight;
	this.forceTopForIE = forceTopForIE;
	this.changeHandler = changeHandler;

	this.left = document.getElementById(leftDiv);
	this.slider = document.getElementById(sliderDiv);
	this.right = document.getElementById(rightDiv);
	this.parent = this.left.parentNode;

	this.slider.hs = this;
	this.slider.onmousedown = startHorizontalDrag;

	this.positionSlider();
}

HorizontalSplit.prototype.positionSlider = function() {
	var bodyPos = findObject(document.body);
	var parentPos = findObject(this.parent);

	var top =  parentPos.y;
	if (IE && this.forceTopForIE) top = 0; //kludge

	var height = parentPos.h;
	if (this.fillHeight) {
		//See if there is an uncle, and assume that it is
		//a footer div. Get its height and set the height
		//of the parent to all the space between the top
		//of the parent and the top of the footer.
		var uncle = this.parent.nextSibling;
		while (uncle && (uncle.nodeType != 1)) uncle = uncle.nextSibling;
		if (uncle && ( !uncle.style || !uncle.style.zIndex || (uncle.style.zIndex < 10) )) {
			var unclePos = findObject(uncle);
			height = bodyPos.h - parentPos.y - unclePos.h;
		}
		else {
			//No uncle; just fill the rest of the window
			height = bodyPos.h - parentPos.y;
		}
		if (height < 50) height = 50;
		this.parent.style.height = height + "px";
	}
	if (height < 50) height = 50;

	var w = parentPos.w;
	if (w <= 0) w = bodyPos.w;
	if (this.leftWidth < this.lmin) this.leftWidth = this.lmin;
	if (this.leftWidth > w - this.sliderWidth - this.rmin) this.leftWidth = w - this.sliderWidth - this.rmin;
	if (this.leftWidth < 1) this.leftWidth = 1;

	this.left.style.top = top + "px";
	this.slider.style.top = top + "px";
	this.right.style.top = top + "px";

	this.left.style.height = height + "px";
	this.slider.style.height = height + "px";
	this.right.style.height = height + "px";

	this.left.style.left = 0;
	this.left.style.width = this.leftWidth + "px";

	this.slider.style.left = this.leftWidth + "px";
	this.slider.style.width = this.sliderWidth + "px";

	var rl = this.leftWidth + this.sliderWidth;
	var rw = w - rl;
	if (rw < 50) rw = 50;
	this.right.style.left = rl + "px";
	this.right.style.width = rw + "px";

	if (this.changeHandler) this.changeHandler();
}

HorizontalSplit.prototype.toString = function() {
	var s = "";
	s += getDivParams("parent", this.parent);
	s += getDivParams("left", this.left);
	s += getDivParams("slider", this.slider);
	s += getDivParams("right", this.right);
	return s;
}

HorizontalSplit.prototype.setSlider = function(position) {
	this.leftWidth = position;
	this.positionSlider();
}

HorizontalSplit.prototype.moveSlider = function(increment) {
	this.leftWidth += increment;
	this.positionSlider();
}

HorizontalSplit.prototype.moveSliderTo = function(position) {
	var nsteps = 30;
	position = (position > this.lmin) ? position : this.lmin;
	var delta = (position - this.leftWidth) / nsteps;
	var n = 1;
	var interval = 1;
	var hs = this;
	move();

	function move() {
		if (n > nsteps) {
			hs.leftWidth = position;
			clearTimeout(timer);
		}
		else if (hs.leftWidth != position) {
			hs.leftWidth += delta;
			n++
			timer = setTimeout(move, interval);
		}
		hs.positionSlider();
	}
}

function startHorizontalDrag(evt) {
	if (!evt) evt = window.event;
	var source = getSource(evt);
	var hs = source.hs;
	var deltaX = evt.clientX - parseInt(hs.slider.style.left);
	var deltaY = evt.clientY - parseInt(hs.slider.style.top);
	if (document.addEventListener) {
		document.addEventListener("mousemove", dragSlider, true);
		document.addEventListener("mouseup", dropSlider, true);
	}
	else {
		hs.slider.attachEvent("onmousemove", dragSlider);
		hs.slider.attachEvent("onmouseup", dropSlider);
		hs.slider.setCapture();
	}
	if (evt.stopPropagation) evt.stopPropagation();
	evt.cancelBubble = true;
	if (evt.preventDefault) evt.preventDefault();
	evt.returnValue = false;
	return false;

	function dragSlider(evt) {
		if (!evt) evt = window.event;
		hs.leftWidth = (evt.clientX - deltaX);
		hs.positionSlider();
		if (evt.stopPropagation) evt.stopPropagation();
		evt.cancelBubble = true;
		return false;
	}

	function dropSlider(evt) {
		if (!evt) evt = window.event;
		if (document.addEventListener) {
			document.removeEventListener("mouseup", dropSlider, true);
			document.removeEventListener("mousemove", dragSlider, true);
		}
		else {
			hs.slider.detachEvent("onmousemove", dragSlider);
			hs.slider.detachEvent("onmouseup", dropSlider);
			hs.slider.releaseCapture();
		}
		if (evt.stopPropagation) evt.stopPropagation();
		evt.cancelBubble = true;
		return false;
	}
}

function VerticalSplit(topDiv, sliderDiv, bottomDiv,
						sliderPosition,
						topMin, bottomMin,
						changeHandler) {
	this.sliderHeight = 6;
	this.tmin = (topMin ? topMin : 120);
	this.bmin = (bottomMin ? bottomMin : 120);
	this.topHeight = (sliderPosition ? sliderPosition : 300);
	this.changeHandler = changeHandler;

	this.top = document.getElementById(topDiv);
	this.slider = document.getElementById(sliderDiv);
	this.bottom = document.getElementById(bottomDiv);
	this.parent = this.top.parentNode;

	this.slider.vs = this;
	this.slider.onmousedown = startVerticalDrag;

	this.positionSlider();
}

VerticalSplit.prototype.positionSlider = function() {
	var parentPos = findObject(this.parent);

	var left =  parentPos.x;
	var width = parentPos.w;

	if (this.topHeight < this.tmin) this.topHeight = this.tmin;
	if (this.topHeight > parentPos.h - this.sliderHeight - this.bmin) this.topHeight = parentPos.h - this.sliderHeight - this.bmin;
	if (this.topHeight < 1) this.topHeight = 1;

	this.top.style.left = left + "px";
	this.slider.style.left = left + "px";
	this.bottom.style.left = left + "px";

	this.top.style.width = width + "px";
	this.slider.style.width = width + "px";
	this.bottom.style.width = width + "px";

	this.top.style.top = 0 + "px";
	this.top.style.height = this.topHeight + "px";

	this.slider.style.top = this.topHeight + "px";
	this.slider.style.height = this.sliderHeight + "px";

	var bt = this.topHeight + this.sliderHeight;
	var bh = parentPos.h - bt;
	if (bh < 1) bh = 1;
	this.bottom.style.top = bt + "px";
	this.bottom.style.height = bh + "px";

	if (this.changeHandler) this.changeHandler();
}

VerticalSplit.prototype.toString = function() {
	var s = "";
	s += getDivParams("parent", this.parent);
	s += getDivParams("top", this.top);
	s += getDivParams("slider", this.slider);
	s += getDivParams("bottom", this.bottom);
	return s;
}

function getDivParams(name, div) {
	var s = "";
	s += name+".id = "+div.id+"\n";
	s += name+".style.top = "+div.style.top+"\n";
	s += name+".style.left = "+div.style.left+"\n";
	s += name+".style.width = "+div.style.width+"\n";
	s += name+".style.height = "+div.style.height+"\n";
	s += name+".className = "+div.className+"\n\n";
	return s;
}

function startVerticalDrag(evt) {
	if (!evt) evt = window.event;
	var source = getSource(evt);
	var vs = source.vs;
	var deltaX = evt.clientX - parseInt(vs.slider.style.left);
	var deltaY = evt.clientY - parseInt(vs.slider.style.top);
	if (document.addEventListener) {
		document.addEventListener("mousemove", dragSlider, true);
		document.addEventListener("mouseup", dropSlider, true);
	}
	else {
		vs.slider.attachEvent("onmousemove", dragSlider);
		vs.slider.attachEvent("onmouseup", dropSlider);
		vs.slider.setCapture();
	}
	if (evt.stopPropagation) evt.stopPropagation();
	evt.cancelBubble = true;
	if (evt.preventDefault) evt.preventDefault();
	evt.returnValue = false;
	return false;

	function dragSlider(evt) {
		if (!evt) evt = window.event;
		vs.topHeight = (evt.clientY - deltaY);
		vs.positionSlider();
		if (evt.stopPropagation) evt.stopPropagation();
		evt.cancelBubble = true;
		return false;
	}

	function dropSlider(evt) {
		if (!evt) evt = window.event;
		if (document.addEventListener) {
			document.removeEventListener("mouseup", dropSlider, true);
			document.removeEventListener("mousemove", dragSlider, true);
		}
		else {
			vs.slider.detachEvent("onmousemove", dragSlider);
			vs.slider.detachEvent("onmouseup", dropSlider);
			vs.slider.releaseCapture();
		}
		if (evt.stopPropagation) evt.stopPropagation();
		evt.cancelBubble = true;
		return false;
	}
}

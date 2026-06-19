// Shared form validation for module Start buttons.
//
// Each module declares its required fields as a list of requirements and calls
// updateStartButton(). The Start button is enabled only when every requirement is
// met; the unmet ones are listed beside the button AND marked in place — a red ring on
// the field plus "(Required)" appended to its title — so it's obvious what's missing.

// Enable a module's Start button only when every required field is met. Shows the unmet
// labels beside the button AND marks each unmet field in place. `requirements`:
//   [{ label, met, field?, note? }]. Returns whether the form is valid.
function updateStartButton(buttonId, requirements) {
    const button = document.getElementById(buttonId);
    if (!button) return false;

    const missing = [];
    requirements.forEach(r => {
        markField(r);                       // in-place ring + "(Required)" on the field
        if (!r.met && r.show !== false) missing.push(r.label);
    });
    const valid = requirements.every(r => r.met);

    button.disabled = !valid;
    button.classList.toggle('opacity-50', !valid);
    button.classList.toggle('cursor-not-allowed', !valid);

    const message = document.getElementById(buttonId + '_missing');
    if (message) {
        message.textContent = valid ? '' : 'Required to start: ' + missing.join(', ');
        message.classList.toggle('hidden', valid);
    }
    return valid;
}

// Resolve a requirement's anchor element: '#id'/'.cls'/'a b'/'[attr]' → querySelector,
// otherwise treat it as a field name.
function requirementField(r) {
    if (!r.field) return null;
    const bySelector = /[#.\s\[]/.test(r.field);
    return document.querySelector(bySelector ? r.field : '[name="' + r.field + '"]');
}

// The label element whose text gets the "(Required)" suffix. An explicit r.title
// selector wins; otherwise walk up from the field to the nearest preceding sibling —
// this finds the title whether the input sits right after it or inside a layout wrapper.
function requirementTitle(r, el) {
    if (r.title) return document.querySelector(r.title);
    let node = el;
    while (node) {
        if (node.previousElementSibling) return node.previousElementSibling;
        node = node.parentElement;
    }
    return null;
}

// Mark a requirement on its field. The "(Required)" suffix on the title stays visible
// while the requirement applies — red when the field is empty, black once it's filled —
// and the empty field gets a red left line + gentle red background (matching the column
// rows), cleared once it's filled. A requirement with `show: false` is suppressed
// entirely. The suffix span is created once and reused (idempotent across re-validations).
function markField(r) {
    const show = r.show !== false;
    const el = requirementField(r);
    if (el) {
        const highlight = show && !r.met;
        el.classList.toggle('!border-l-4', highlight);
        el.classList.toggle('!border-l-red-600', highlight);
        el.classList.toggle('bg-red-50', highlight);
    }

    const title = requirementTitle(r, el);
    if (title) {
        let suffix = title.querySelector('.required-suffix');
        if (!suffix) {
            suffix = document.createElement('span');
            suffix.className = 'required-suffix';
            suffix.textContent = ' (Required)';
            title.appendChild(suffix);
        }
        suffix.classList.toggle('hidden', !show);
        suffix.classList.toggle('text-red-600', show && !r.met);   // empty → red
        suffix.classList.toggle('text-black', show && r.met);      // populated → black
    }
}

// Trimmed value of an input by name (most fields are addressed by [name=...]).
function fieldValue(name) {
    const el = document.querySelector('[name="' + name + '"]');
    return el ? el.value.trim() : '';
}

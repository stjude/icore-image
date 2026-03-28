"""Translate CTP anonymizer/pixel/filter scripts to dicom-deid-rs recipe format."""

import re
import xml.etree.ElementTree as ET


# ---------------------------------------------------------------------------
# Anonymizer XML translation
# ---------------------------------------------------------------------------


def translate_anonymizer_script(xml_text: str) -> tuple[list[str], dict[str, str]]:
    """Translate a CTP anonymizer XML script to recipe header lines.

    Returns (header_lines, variables) where variables is a dict of <p> params.
    """
    params: dict[str, str] = {}
    lines: list[str] = []

    root = ET.fromstring(xml_text)

    for elem in root.findall("p"):
        name = elem.get("t")
        if name and elem.text:
            params[name] = elem.text.strip()

    for elem in root.findall("e"):
        enabled = elem.get("en", "T")
        if enabled == "F":
            continue

        tag_hex = elem.get("t", "")
        tag_name = elem.get("n")
        tag = _format_tag_identifier(tag_name, tag_hex)
        action_text = (elem.text or "").strip()

        line = _translate_action(action_text, tag)
        if line is not None:
            lines.append(line)

    for elem in root.findall("r"):
        enabled = elem.get("en", "T")
        if enabled == "F":
            continue
        rule_type = elem.get("t", "")
        lines.extend(_removal_rule_lines(rule_type))

    return lines, params


def _format_tag_identifier(tag_name: str | None, tag_hex: str) -> str:
    # Always prefer the hex tag format (GGGG,EEEE) since CTP tag names
    # don't always match the DICOM dictionary exactly (e.g. CTP uses
    # "DeIdentificationMethod" but DICOM uses "DeidentificationMethod").
    if len(tag_hex) == 8 and all(c in "0123456789abcdefABCDEF" for c in tag_hex):
        return f"({tag_hex[:4]},{tag_hex[4:]})"
    if tag_name:
        return tag_name
    return tag_hex


def _translate_action(action_text: str, tag: str) -> str | None:
    if not action_text:
        return f"BLANK {tag}"

    if action_text == "@keep()":
        return f"KEEP {tag}"
    if action_text == "@remove()":
        return f"REMOVE {tag}"
    if action_text == "@empty()":
        return f"BLANK {tag}"
    if action_text == "@require()":
        return None
    if action_text.startswith("@hashuid("):
        return f"REPLACE {tag} func:hashuid"
    if action_text.startswith("@hashname("):
        return f"REPLACE {tag} func:hashname"
    if action_text.startswith("@hashdate("):
        return f"REPLACE {tag} func:hashdate"
    if action_text.startswith("@hashptid("):
        return f"REPLACE {tag} func:hashptid"
    if action_text.startswith("@hash("):
        return f"REPLACE {tag} func:hash"
    if action_text.startswith("@incrementdate("):
        var_name = _extract_increment_param(action_text)
        return f"JITTER {tag} var:{var_name}"
    if action_text.startswith("@always()"):
        value = action_text[len("@always()") :]
        return f"ADD {tag} {value}"
    if action_text.startswith("@param("):
        var_name = _extract_param_ref(action_text)
        if var_name:
            return f"REPLACE {tag} var:{var_name}"
        return None
    if action_text.startswith("@lookup("):
        return f"REPLACE {tag} func:lookup"

    return None


def _extract_increment_param(text: str) -> str:
    args = _extract_function_args(text)
    if args:
        parts = args.split(",")
        if len(parts) >= 2:
            param_ref = parts[1].strip()
            if param_ref.startswith("@"):
                return param_ref[1:]
    return "DATEINC"


def _extract_param_ref(text: str) -> str | None:
    args = _extract_function_args(text)
    if args:
        trimmed = args.strip()
        if trimmed.startswith("@"):
            return trimmed[1:]
    return None


def _extract_function_args(text: str) -> str | None:
    start = text.find("(")
    end = text.rfind(")")
    if start is not None and end is not None and start < end:
        return text[start + 1 : end]
    return None


def _removal_rule_lines(rule_type: str) -> list[str]:
    if rule_type == "overlays":
        return ["REMOVE OverlayData"]
    if rule_type == "curves":
        return ["# Remove curves (retired 50xx groups)"]
    if rule_type == "uncheckedUIDs":
        return ["# Remove unchecked UIDs"]
    if rule_type == "privategroups":
        return ["# Remove private groups"]
    return [f"# Remove {rule_type}"]


# ---------------------------------------------------------------------------
# Pixel anonymizer script translation
# ---------------------------------------------------------------------------

_COORD_RE = re.compile(r"\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)")

_PREDICATE_RE = re.compile(
    r'^(!?)(\[[\d\w,]+\]|\w+)\.'
    r'(containsIgnoreCase|equals|equalsIgnoreCase|startsWith|startsWithIgnoreCase)'
    r'\("([^"]*)"\)$'
)


def translate_pixel_anonymizer_script(script_text: str) -> list[str]:
    """Translate a CTP pixel anonymizer script to recipe graylist filter lines."""
    blocks = _parse_pixel_blocks(script_text)
    lines: list[str] = []

    for block in blocks:
        if lines:
            lines.append("")
        lines.append(f"LABEL {block['label']}")
        for i, (op, predicate) in enumerate(block["conditions"]):
            if i == 0:
                prefix = "  "
            elif op == "and":
                prefix = "  + "
            else:
                prefix = "  || "
            lines.append(f"{prefix}{predicate}")
        for x, y, w, h in block["coordinates"]:
            lines.append(f"  ctpcoordinates {x},{y},{w},{h}")

    return lines


def _parse_pixel_blocks(script: str) -> list[dict]:
    blocks = []
    lines = script.splitlines()
    i = 0
    title_lines: list[str] = []

    while i < len(lines):
        trimmed = lines[i].strip()

        if not trimmed:
            i += 1
            continue

        if "{" in trimmed:
            condition_text = ""
            j = i
            while j < len(lines):
                line = lines[j].strip()
                condition_text += " " + line
                if "}" in line:
                    j += 1
                    break
                j += 1

            cond_start = condition_text.find("{")
            cond_end = condition_text.rfind("}")
            cond_text = condition_text[cond_start + 1 : cond_end].strip()

            coordinates = []
            while j < len(lines):
                line = lines[j].strip()
                if not line:
                    j += 1
                    continue
                if line.startswith("("):
                    coordinates.extend(_extract_all_coordinates(line))
                    j += 1
                else:
                    break

            conditions = _parse_pixel_conditions(cond_text)

            if title_lines:
                label = title_lines[-1]
            else:
                label = _generate_label_from_conditions(conditions)

            blocks.append(
                {
                    "label": label,
                    "conditions": conditions,
                    "coordinates": coordinates,
                }
            )

            title_lines.clear()
            i = j
        elif trimmed.startswith("("):
            i += 1
        else:
            title_lines.append(trimmed)
            i += 1

    return blocks


def _extract_all_coordinates(text: str) -> list[tuple[int, int, int, int]]:
    return [
        (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        for m in _COORD_RE.finditer(text)
    ]


def _parse_pixel_conditions(text: str) -> list[tuple[str, str]]:
    parts = _split_condition_parts(text)
    result = []
    for i, (op, raw) in enumerate(parts):
        if i == 0:
            op = "first"
        predicate = _translate_pixel_predicate(raw.strip())
        if predicate:
            result.append((op, predicate))
    return result


def _split_condition_parts(text: str) -> list[tuple[str, str]]:
    result = []
    current = ""
    in_quotes = False
    current_op = "first"

    for ch in text:
        if ch == '"':
            in_quotes = not in_quotes
            current += ch
        elif ch == "*" and not in_quotes:
            piece = current.strip()
            if piece:
                result.append((current_op, piece))
            current_op = "and"
            current = ""
        elif ch == "+" and not in_quotes:
            piece = current.strip()
            if piece:
                result.append((current_op, piece))
            current_op = "or"
            current = ""
        else:
            current += ch

    piece = current.strip()
    if piece:
        result.append((current_op, piece))

    return result


def _translate_pixel_predicate(raw: str) -> str | None:
    m = _PREDICATE_RE.match(raw)
    if not m:
        return None

    negated = m.group(1) == "!"
    tag_raw = m.group(2)
    method = m.group(3)
    value = m.group(4)

    # Convert [GGGG,EEEE] to bare hex GGGGEEEE
    if tag_raw.startswith("[") and tag_raw.endswith("]"):
        tag = tag_raw[1:-1].replace(",", "")
    else:
        tag = tag_raw

    if method in ("containsIgnoreCase", "startsWith", "startsWithIgnoreCase"):
        if method in ("startsWith", "startsWithIgnoreCase"):
            return f"{'notstartswith' if negated else 'startswith'} {tag} {value}"
        return f"{'notcontains' if negated else 'contains'} {tag} {value}"
    elif method in ("equals", "equalsIgnoreCase"):
        return f"{'notequals' if negated else 'equals'} {tag} {value}"

    return None


def _generate_label_from_conditions(
    conditions: list[tuple[str, str]],
) -> str:
    fields = []
    for _, pred in conditions[:3]:
        parts = pred.split(None, 2)
        if len(parts) >= 2:
            fields.append(parts[1])
    return " ".join(fields) if fields else "Unknown"


# ---------------------------------------------------------------------------
# CTP filter script translation (to whitelist)
# ---------------------------------------------------------------------------

_FILTER_PRED_RE = re.compile(
    r'(!?)(\[[\d\w,]+\]|\w+)\.'
    r'(equals|equalsIgnoreCase|matches|contains|containsIgnoreCase|'
    r'startsWith|startsWithIgnoreCase|endsWith|endsWithIgnoreCase|'
    r'isLessThan|isGreaterThan)'
    r'\("([^"]*)"\)'
)


def translate_filter_script(filter_text: str) -> list[str]:
    """Translate a CTP filter script to recipe whitelist filter lines.

    CTP filters are whitelists: files matching the expression pass through.
    This translates to %filter whitelist labels in the recipe format.
    """
    filter_text = filter_text.strip()
    if not filter_text or filter_text == "true.":
        return []

    # Extract all predicates from the filter expression and group them
    # into labels. Each top-level OR group becomes a separate LABEL.
    labels = _parse_filter_expression(filter_text)
    lines: list[str] = []

    for i, label in enumerate(labels):
        if lines:
            lines.append("")
        lines.append(f"LABEL filter_rule_{i}")
        for j, (op, predicate) in enumerate(label):
            if j == 0:
                prefix = "  "
            elif op == "and":
                prefix = "  + "
            else:
                prefix = "  || "
            lines.append(f"{prefix}{predicate}")

    return lines


def _parse_filter_expression(text: str) -> list[list[tuple[str, str]]]:
    """Parse a CTP filter expression into groups of conditions.

    Returns a list of labels, where each label is a list of (op, predicate) tuples.
    Top-level + (OR in CTP) creates separate labels.
    """
    # Remove outer parentheses if they wrap the entire expression
    text = text.strip()
    text = _strip_outer_parens(text)

    # Split on top-level + (CTP OR operator) respecting parentheses
    or_groups = _split_top_level(text, "+")

    labels = []
    for group in or_groups:
        group = _strip_outer_parens(group.strip())
        conditions = _extract_filter_conditions(group)
        if conditions:
            labels.append(conditions)

    return labels


def _strip_outer_parens(text: str) -> str:
    """Remove outer matching parentheses if present."""
    text = text.strip()
    while text.startswith("(") and _matching_paren(text, 0) == len(text) - 1:
        text = text[1:-1].strip()
    return text


def _matching_paren(text: str, start: int) -> int:
    """Find the matching closing paren for the opening paren at `start`."""
    depth = 0
    in_quotes = False
    for i in range(start, len(text)):
        if text[i] == '"':
            in_quotes = not in_quotes
        elif not in_quotes:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    return i
    return -1


def _split_top_level(text: str, sep: str) -> list[str]:
    """Split text on `sep` only at the top nesting level."""
    parts = []
    current = ""
    depth = 0
    in_quotes = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == '"':
            in_quotes = not in_quotes
            current += ch
        elif not in_quotes:
            if ch == "(":
                depth += 1
                current += ch
            elif ch == ")":
                depth -= 1
                current += ch
            elif depth == 0 and text[i:].startswith(sep):
                parts.append(current)
                current = ""
                i += len(sep)
                continue
            else:
                current += ch
        else:
            current += ch
        i += 1
    if current.strip():
        parts.append(current)
    return parts


def _extract_filter_conditions(text: str) -> list[tuple[str, str]]:
    """Extract conditions from a CTP filter expression (within one OR group).

    Within a group, * is AND.
    """
    # Split on * (CTP AND)
    and_parts = _split_top_level(text, "*")
    conditions = []

    for i, part in enumerate(and_parts):
        part = _strip_outer_parens(part.strip())

        # Check for nested OR groups within this AND part
        or_subparts = _split_top_level(part, "+")
        if len(or_subparts) > 1:
            # Nested OR within AND -- add each as || conditions
            for j, subpart in enumerate(or_subparts):
                subpart = _strip_outer_parens(subpart.strip())
                preds = _extract_predicates_from_atom(subpart)
                for k, pred in enumerate(preds):
                    if i == 0 and j == 0 and k == 0:
                        op = "first"
                    elif j > 0 and k == 0:
                        op = "or"
                    else:
                        op = "and"
                    conditions.append((op, pred))
        else:
            preds = _extract_predicates_from_atom(part)
            for k, pred in enumerate(preds):
                if i == 0 and k == 0:
                    op = "first"
                else:
                    op = "and"
                conditions.append((op, pred))

    return conditions


def _extract_predicates_from_atom(text: str) -> list[str]:
    """Extract predicate strings from an atomic filter expression."""
    results = []
    for m in _FILTER_PRED_RE.finditer(text):
        negated = m.group(1) == "!"
        tag_raw = m.group(2)
        method = m.group(3)
        value = m.group(4)

        if tag_raw.startswith("[") and tag_raw.endswith("]"):
            tag = tag_raw[1:-1].replace(",", "")
        else:
            tag = tag_raw

        if method in ("startsWith", "startsWithIgnoreCase"):
            kw = "notstartswith" if negated else "startswith"
            results.append(f"{kw} {tag} {value}")
        elif method in ("contains", "containsIgnoreCase"):
            kw = "notcontains" if negated else "contains"
            results.append(f"{kw} {tag} {value}")
        elif method in ("equals", "equalsIgnoreCase"):
            kw = "notequals" if negated else "equals"
            results.append(f"{kw} {tag} {value}")
        elif method in ("endsWith", "endsWithIgnoreCase"):
            # Approximate with contains
            kw = "notcontains" if negated else "contains"
            results.append(f"{kw} {tag} {value}")
        elif method == "matches":
            kw = "notcontains" if negated else "contains"
            results.append(f"{kw} {tag} {value}")
        elif method in ("isLessThan", "isGreaterThan"):
            # No direct equivalent -- skip
            pass

    return results


# ---------------------------------------------------------------------------
# Recipe assembly
# ---------------------------------------------------------------------------


def build_recipe(
    anonymizer_xml: str | None = None,
    pixel_script: str | None = None,
    filter_script: str | None = None,
) -> tuple[str, dict[str, str]]:
    """Build a complete dicom-deid-rs recipe from CTP scripts.

    Returns (recipe_text, variables_dict).
    """
    output = "FORMAT dicom\n"
    variables: dict[str, str] = {}

    if filter_script:
        filter_lines = translate_filter_script(filter_script)
        if filter_lines:
            output += "\n%filter whitelist\n"
            output += "\n"
            output += "\n".join(filter_lines)
            output += "\n"

    if pixel_script:
        pixel_lines = translate_pixel_anonymizer_script(pixel_script)
        if pixel_lines:
            output += "\n%filter graylist\n"
            output += "\n"
            output += "\n".join(pixel_lines)
            output += "\n"

    if anonymizer_xml:
        header_lines, params = translate_anonymizer_script(anonymizer_xml)
        variables.update(params)
        if header_lines:
            output += "\n%header\n"
            output += "\n"
            output += "\n".join(header_lines)
            output += "\n"

    return output, variables

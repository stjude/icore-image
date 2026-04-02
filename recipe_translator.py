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
    """Format a DICOM tag as ``(GGGG,EEEE)`` when *tag_hex* is 8 hex digits.

    Falls back to *tag_name*, then to the raw *tag_hex* string.

    >>> _format_tag_identifier("PatientName", "00100010")
    '(0010,0010)'
    >>> _format_tag_identifier("PatientName", "short")
    'PatientName'
    >>> _format_tag_identifier(None, "short")
    'short'
    """
    if len(tag_hex) == 8 and all(c in "0123456789abcdefABCDEF" for c in tag_hex):
        return f"({tag_hex[:4]},{tag_hex[4:]})"
    if tag_name:
        return tag_name
    return tag_hex


def _translate_action(action_text: str, tag: str) -> str | None:
    """Map a CTP action string to a recipe directive for *tag*.

    >>> _translate_action("", "PatientName")
    'BLANK PatientName'
    >>> _translate_action("@keep()", "Modality")
    'KEEP Modality'
    >>> _translate_action("@remove()", "PatientID")
    'REMOVE PatientID'
    >>> _translate_action("@empty()", "StudyDate")
    'BLANK StudyDate'
    >>> _translate_action("@require()", "Modality")
    'REQUIRE Modality'
    >>> _translate_action("remove", "Tag")
    'REMOVE Tag'
    >>> _translate_action("@hashuid(this,@UIDROOT)", "StudyInstanceUID")
    'REPLACE StudyInstanceUID func:hashuid'
    >>> _translate_action("@hash(this,5)", "PatientID")
    'REPLACE PatientID func:hash'
    >>> _translate_action("@incrementdate(this,@DATEINC)", "StudyDate")
    'JITTER StudyDate var:DATEINC'
    >>> _translate_action("@always()ANON", "InstitutionName")
    'ADD InstitutionName ANON'
    >>> _translate_action("@param(@SITEID)", "ClinicalTrialSiteID")
    'REPLACE ClinicalTrialSiteID var:SITEID'
    >>> _translate_action("@lookup(this,ptid)", "PatientID")
    'REPLACE PatientID func:lookup'
    >>> _translate_action("@append(){CTP: v1}", "DeIdentificationMethod")
    'APPEND DeIdentificationMethod {CTP: v1}'
    >>> _translate_action("YES", "PatientIdentityRemoved")
    'REPLACE PatientIdentityRemoved YES'
    """
    if not action_text:
        return f"BLANK {tag}"

    if action_text == "@keep()":
        return f"KEEP {tag}"
    if action_text == "@remove()":
        return f"REMOVE {tag}"
    if action_text == "@empty()":
        return f"BLANK {tag}"
    if action_text == "@require()":
        return f"REQUIRE {tag}"
    if action_text == "remove":
        return f"REMOVE {tag}"
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
    if action_text.startswith("@append()"):
        value = action_text[len("@append()") :]
        return f"APPEND {tag} {value}"
    if action_text.startswith("@if("):
        return _translate_if_action(action_text, tag)

    # Bare literal value -- set it directly
    return f"REPLACE {tag} {action_text}"


def _extract_increment_param(text: str) -> str:
    """Extract the variable name from an ``@incrementdate`` call.

    The second argument (after ``this``) is a ``@VAR`` reference.
    Falls back to ``"DATEINC"`` when the pattern doesn't match.

    >>> _extract_increment_param("@incrementdate(this,@DATEINC)")
    'DATEINC'
    >>> _extract_increment_param("@incrementdate(this,@MY_OFFSET)")
    'MY_OFFSET'
    >>> _extract_increment_param("@incrementdate(this)")
    'DATEINC'
    """
    args = _extract_function_args(text)
    if args:
        parts = args.split(",")
        if len(parts) >= 2:
            param_ref = parts[1].strip()
            if param_ref.startswith("@"):
                return param_ref[1:]
    return "DATEINC"


def _extract_param_ref(text: str) -> str | None:
    """Extract the variable name from a ``@param(@VAR)`` call.

    >>> _extract_param_ref("@param(@SITEID)")
    'SITEID'
    >>> _extract_param_ref("@param(@PROJECTNAME)")
    'PROJECTNAME'
    >>> _extract_param_ref("@param(noatsign)") is None
    True
    """
    args = _extract_function_args(text)
    if args:
        trimmed = args.strip()
        if trimmed.startswith("@"):
            return trimmed[1:]
    return None


_IF_RE = re.compile(
    r"@if\(([^)]+)\)\{([^}]*)\}(?:\{([^}]*)\})?"
)


def _translate_if_action(action_text: str, tag: str) -> str | None:
    """Translate ``@if(...){...}{...}`` conditionals to recipe format.

    >>> _translate_if_action('@if(this,isblank){@remove()}{Removed by CTP}', 'Name')
    'REPLACE_IF_NOT_BLANK Name Removed by CTP'
    >>> _translate_if_action('@if(this,isblank){@remove()}', 'Name')
    'REMOVE_IF_BLANK Name'
    >>> _translate_if_action('@if(ImageType,contains,"SCREEN SAVE"){@quarantine()}{@keep()}', 'IT')
    'QUARANTINE_IF IT contains SCREEN SAVE'
    >>> _translate_if_action('@if(ImageType,contains,"X"){@keep()}{@keep()}', 'IT')
    'KEEP IT'
    >>> _translate_if_action('@if(unknown,op){foo}', 'T')
    '# UNSUPPORTED @if: @if(unknown,op){foo}'
    """
    m = _IF_RE.match(action_text)
    if not m:
        return f"# UNSUPPORTED @if: {action_text}"
    condition = m.group(1)
    true_branch = m.group(2).strip()
    false_branch = (m.group(3) or "").strip()

    # @if(this,isblank){@remove()}{literal} -- conditional remove/replace
    parts = [p.strip() for p in condition.split(",")]
    if len(parts) >= 2 and parts[1] == "isblank":
        if true_branch == "@remove()":
            if false_branch:
                return f"REPLACE_IF_NOT_BLANK {tag} {false_branch}"
            return f"REMOVE_IF_BLANK {tag}"

    # @if(Tag,contains,"value"){action}{action}
    if len(parts) >= 3 and parts[1] == "contains":
        value = parts[2].strip('"')
        if true_branch == "@quarantine()":
            return f"QUARANTINE_IF {tag} contains {value}"
        if true_branch == "@keep()":
            if false_branch == "@keep()":
                return f"KEEP {tag}"

    return f"# UNSUPPORTED @if: {action_text}"


def _extract_function_args(text: str) -> str | None:
    """Return the substring between the first ``(`` and last ``)``.

    >>> _extract_function_args("@hash(this,5)")
    'this,5'
    >>> _extract_function_args("@param(@SITEID)")
    '@SITEID'
    >>> _extract_function_args("noparens") is None
    True
    """
    start = text.find("(")
    end = text.rfind(")")
    if start is not None and end is not None and start < end:
        return text[start + 1 : end]
    return None


def _removal_rule_lines(rule_type: str) -> list[str]:
    """Convert a CTP ``<r>`` removal rule type to recipe lines.

    >>> _removal_rule_lines("overlays")
    ['REMOVE OverlayData']
    >>> _removal_rule_lines("curves")
    ['# Remove curves (retired 50xx groups)']
    >>> _removal_rule_lines("privategroups")
    ['# Remove private groups']
    >>> _removal_rule_lines("uncheckedUIDs")
    ['# Remove unchecked UIDs']
    >>> _removal_rule_lines("something")
    ['# Remove something']
    """
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
    r"^(!?)(\[[\d\w,]+\]|\w+)\."
    r"(containsIgnoreCase|equals|equalsIgnoreCase|startsWith|startsWithIgnoreCase)"
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
    """Parse a CTP pixel anonymizer script into structured blocks.

    Each block has a ``label``, ``conditions`` list, and ``coordinates`` list.

    >>> blocks = _parse_pixel_blocks('''
    ... GE CT
    ... { Manufacturer.containsIgnoreCase("GE") }
    ... (0,0,100,20)
    ... ''')
    >>> len(blocks)
    1
    >>> blocks[0]['label']
    'GE CT'
    >>> blocks[0]['coordinates']
    [(0, 0, 100, 20)]
    """
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
    """Extract all ``(x, y, w, h)`` coordinate tuples from a line.

    >>> _extract_all_coordinates("(0, 0, 100, 20)")
    [(0, 0, 100, 20)]
    >>> _extract_all_coordinates("(1,2,3,4) (10,20,30,40)")
    [(1, 2, 3, 4), (10, 20, 30, 40)]
    >>> _extract_all_coordinates("no coords here")
    []
    """
    return [
        (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        for m in _COORD_RE.finditer(text)
    ]


def _parse_pixel_conditions(text: str) -> list[tuple[str, str]]:
    """Parse a pixel condition string into ``(operator, predicate)`` pairs.

    ``*`` is AND, ``+`` is OR.  The first entry always gets operator
    ``"first"``.

    >>> _parse_pixel_conditions('Manufacturer.containsIgnoreCase("GE")')
    [('first', 'contains Manufacturer GE')]
    >>> _parse_pixel_conditions(
    ...     'Manufacturer.containsIgnoreCase("GE") * Modality.equals("CT")'
    ... )
    [('first', 'contains Manufacturer GE'), ('and', 'equals Modality CT')]
    """
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
    """Split a condition string on ``*`` (AND) and ``+`` (OR) operators.

    Respects quoted strings so that operators inside quotes are not
    treated as separators.

    >>> _split_condition_parts('A * B')
    [('first', 'A'), ('and', 'B')]
    >>> _split_condition_parts('A + B')
    [('first', 'A'), ('or', 'B')]
    >>> _split_condition_parts('A * B + C')
    [('first', 'A'), ('and', 'B'), ('or', 'C')]
    >>> _split_condition_parts('Tag.equals("a*b")')
    [('first', 'Tag.equals("a*b")')]
    """
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
    """Translate a single CTP pixel predicate to recipe format.

    >>> _translate_pixel_predicate('Manufacturer.containsIgnoreCase("GE")')
    'contains Manufacturer GE'
    >>> _translate_pixel_predicate('!Modality.equals("CT")')
    'notequals Modality CT'
    >>> _translate_pixel_predicate('[0008,0070].startsWith("SIEMENS")')
    'startswith 00080070 SIEMENS'
    >>> _translate_pixel_predicate('not a predicate') is None
    True
    """
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
    """Build a label string from the tag names in the first few conditions.

    >>> _generate_label_from_conditions([("first", "contains Manufacturer GE")])
    'Manufacturer'
    >>> _generate_label_from_conditions([
    ...     ("first", "contains Manufacturer GE"),
    ...     ("and", "equals Modality CT"),
    ... ])
    'Manufacturer Modality'
    >>> _generate_label_from_conditions([])
    'Unknown'
    """
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
    r"(!?)(\[[\d\w,]+\]|\w+)\."
    r"(equals|equalsIgnoreCase|matches|contains|containsIgnoreCase|"
    r"startsWith|startsWithIgnoreCase|endsWith|endsWithIgnoreCase|"
    r"isLessThan|isGreaterThan)"
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
    """Remove outer matching parentheses if present.

    >>> _strip_outer_parens("(hello)")
    'hello'
    >>> _strip_outer_parens("((nested))")
    'nested'
    >>> _strip_outer_parens("(a) * (b)")
    '(a) * (b)'
    >>> _strip_outer_parens("no parens")
    'no parens'
    """
    text = text.strip()
    while text.startswith("(") and _matching_paren(text, 0) == len(text) - 1:
        text = text[1:-1].strip()
    return text


def _matching_paren(text: str, start: int) -> int:
    """Find the index of the closing paren matching the one at *start*.

    Returns ``-1`` if no matching paren is found.

    >>> _matching_paren("(abc)", 0)
    4
    >>> _matching_paren("((a)(b))", 0)
    7
    >>> _matching_paren("((a)(b))", 1)
    3
    >>> _matching_paren("(unmatched", 0)
    -1
    """
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
    """Split *text* on *sep* only at the top parenthesis nesting level.

    Quoted strings and parenthesised groups are kept intact.

    >>> _split_top_level("A * B * C", " * ")
    ['A', 'B', 'C']
    >>> _split_top_level("(A * B) * C", " * ")
    ['(A * B)', 'C']
    >>> _split_top_level('X.equals("*") * Y', " * ")
    ['X.equals("*")', 'Y']
    """
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

    ``*`` is AND within a group; nested ``+`` produces OR conditions.

    >>> _extract_filter_conditions('Modality.equals("CT")')
    [('first', 'equals Modality CT')]
    >>> _extract_filter_conditions(
    ...     'Modality.equals("CT") * ImageType.contains("PRIMARY")'
    ... )
    [('first', 'equals Modality CT'), ('and', 'contains ImageType PRIMARY')]
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
    """Extract predicate strings from an atomic filter expression.

    >>> _extract_predicates_from_atom('Modality.equals("CT")')
    ['equals Modality CT']
    >>> _extract_predicates_from_atom('!ImageType.contains("DERIVED")')
    ['notcontains ImageType DERIVED']
    >>> _extract_predicates_from_atom('[0008,0060].startsWith("MR")')
    ['startswith 00080060 MR']
    >>> _extract_predicates_from_atom('no predicates')
    []
    """
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

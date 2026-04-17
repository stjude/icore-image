# CTP Script Format Specification

This document specifies the script formats used by RSNA's Clinical Trial
Processor (CTP) for DICOM de-identification, pixel anonymization, and
filtering.  It is derived from the CTP Java source code, the RSNA MIRC wiki
documentation, and the scripts shipped with iCore.

---

## 1  DICOM Anonymizer Script

### 1.1  File structure

An anonymizer script is an XML document with `<script>` as the root element.
It contains four kinds of child elements: parameters (`<p>`), element actions
(`<e>`), keep-group rules (`<k>`), and removal rules (`<r>`).

```xml
<script>
  <p t="DATEINC">-3210</p>
  <e en="T" t="00100010" n="PatientName">@hash(this,10)</e>
  <k en="F" t="0018">Keep group 0018</k>
  <r en="T" t="privategroups">Remove private groups</r>
</script>
```

### 1.2  Parameters — `<p>`

Declare named variables that can be referenced from element action scripts.

| Attribute | Meaning |
|-----------|---------|
| `t`       | Parameter name (conventionally uppercase) |

The element text is the parameter value.  CTP predefines the following
parameter names, though any custom name is allowed:

`DATEINC`, `UIDROOT`, `SITEID`, `SITENAME`, `PROJECTNAME`, `TRIALNAME`,
`PROFILENAME`, `SUBJECT`, `PREFIX`, `SUFFIX`, `SPONSOR`, `KEY`.

Parameters are referenced from action scripts with the `@param(@NAME)` syntax
or by prefixing `@NAME` inside function arguments (e.g.
`@incrementdate(this,@DATEINC)`).

### 1.3  Element actions — `<e>`

Specify what to do with an individual DICOM element.

| Attribute | Meaning |
|-----------|---------|
| `en`      | `T` = enabled, `F` = disabled (skip this rule) |
| `t`       | Tag in 8-digit hex (`00100010`) |
| `n`       | Human-readable element name (informational only) |

The element text is the **action script** — a combination of literal text and
function calls that produces the replacement value for the tag.  An empty
action script (blank text) removes the element.

### 1.4  Keep-group rules — `<k>`

Preserve entire DICOM groups from removal by the global "remove unchecked
elements" rule.

| Attribute | Meaning |
|-----------|---------|
| `en`      | `T` = enabled, `F` = disabled |
| `t`       | Group number in hex (e.g. `0018`, `0020`, `0028`) or the keyword `safeprivateelements` |

The element text is a human-readable label (ignored by the engine).

Keep-group rules only take effect when "remove unchecked elements" is also
enabled — they act as exceptions to that global removal.  When
`t="safeprivateelements"`, private elements that are listed in a curated safe
list (maintained by Mallinckrodt Institute) are preserved even when "remove
private groups" is enabled.

### 1.5  Removal rules — `<r>`

Enable global bulk-removal operations on categories of tags.

| Attribute | Meaning |
|-----------|---------|
| `en`      | `T` = enabled, `F` = disabled |
| `t`       | One of the rule types below |

| Rule type              | Effect |
|------------------------|--------|
| `privategroups`        | Remove all elements with odd-numbered groups |
| `unspecifiedelements`   | Remove elements not targeted by any `<e>` rule (except SOPClassUID, SOPInstanceUID, StudyInstanceUID, group 28 pixel parameters, and 60xx overlays) |
| `curves`               | Remove all elements in groups 5000–501E (retired curve data) |
| `overlays`             | Remove all elements in groups 6000–601E (overlay data) |

### 1.6  Precedence

1. A specific `<e>` action on an element overrides any global rule.
2. `<k>` keep-group rules override `<r>` removal rules.
3. Exception: overlay removal (`<r t="overlays">`) overrides keep-group rules
   for overlay groups.

---

## 2  Element addressing

Element names in action scripts (function arguments, `@if` conditions, etc.)
support several formats:

| Format | Example | Notes |
|--------|---------|-------|
| DICOM keyword | `PatientName` | Resolved via the dcm4che tag dictionary |
| 8-digit hex | `00100010` | |
| Parenthesized hex | `(0010,0010)` | |
| Bracketed hex | `[0010,0010]` or `[00100010]` | |
| Private tag by block ID | `0029[BlockID]40` or `002900[BlockID]` | Block ID matches the Private Creator Data Element value (case-insensitive) |
| Sequence navigation | `RefImageSeq::RefSOPClassUID` or `[0008,1140]::(0008,1150)` | `::` separator; the first item dataset of each SQ element is traversed |
| Root dataset reference | `root:PatientID` | Access the root dataset from within an SQ item context |
| `this` | `this` | The element whose replacement value is being built |

---

## 3  Action script functions

Functions are invoked with `@name(args)` syntax.  Multiple functions and
literal text may be combined in one action script.

### 3.1  Basic operations

| Function | Behavior |
|----------|----------|
| `@keep()` | Preserve the element unmodified.  For SQ elements, preserves all item datasets. |
| `@remove()` | Delete the element.  For SQ elements, removes all item datasets. |
| `@empty()` | Replace the value with a zero-length string. |
| `@blank(n)` | Replace the value with *n* space characters. |
| `@require()` | Create the element with an empty value if it does not exist. |
| `@require(ElementName)` | Create the element with the value of *ElementName* if it does not exist. |
| `@require(ElementName,"default")` | Create with *ElementName*'s value, or *default* if that is also absent. |

### 3.2  Content retrieval

| Function | Behavior |
|----------|----------|
| `@contents(ElementName)` | Return the value of *ElementName* (empty string if absent). |
| `@contents(ElementName,"regex")` | Return the value with all regex matches removed. |
| `@contents(ElementName,"regex","replacement")` | Return the value with regex matches replaced. |
| `@value(ElementName)` | Same as `@contents(ElementName)`. |
| `@value(ElementName,"default")` | Return the value of *ElementName*, or *default* if absent/empty. |
| `@truncate(ElementName,n)` | Return the first *n* characters (positive) or last *n* characters (negative). |
| `@pathelement(ElementName,index)` | Split value on `/` and return the segment at *index* (0-based; negative counts from end). |

### 3.3  Case conversion

| Function | Behavior |
|----------|----------|
| `@uppercase(ElementName)` | Convert value to uppercase (empty string if absent). |
| `@lowercase(ElementName)` | Convert value to lowercase (empty string if absent). |

### 3.4  Hashing

| Function | Behavior |
|----------|----------|
| `@hash(ElementName)` | MD5 hash of the element value, returned as a base-10 digit string. |
| `@hash(ElementName,maxlen)` | Same, truncated to *maxlen* characters from the low-order end. |
| `@hashuid(root,ElementName)` | MD5 hash of *ElementName*'s value, converted to base-10, prepended with *root* (period appended if absent). Max 64 chars. |
| `@hashuid(root,ElementName,ElementName2)` | Hash of *ElementName* + anonymized *ElementName2*, prepended with *root*. |
| `@hashname(ElementName,maxlen)` | Combine words, strip whitespace/apostrophes/periods, uppercase, MD5 hash, return *maxlen* base-10 digits. |
| `@hashname(ElementName,maxlen,maxwords)` | Same but only uses the first *maxwords* words. |
| `@hashptid(siteID,ElementName)` | MD5 hash of *siteID* + *ElementName* value, returned as base-10 digits. Typically `@hashptid(@SITEID,this)`. |
| `@hashptid(siteID,ElementName,maxlen)` | Same, truncated to *maxlen*. |
| `@hashdate(ElementName,HashElementName)` | Compute `hash(HashElementName) mod 3650`, negate, add to *ElementName*'s date. Used for per-patient consistent date shifting. |

### 3.5  Date and time

| Function | Behavior |
|----------|----------|
| `@date(separator)` | Current date as `YYYY{sep}MM{sep}DD`.  Empty separator → DICOM format `YYYYMMDD`. |
| `@time(separator)` | Current time as `HH{sep}MM{sep}SS`.  Empty separator → DICOM format `HHMMSS`. |
| `@incrementdate(ElementName,incInDays)` | Add *incInDays* days to the date (positive = future). Accepts `@PARAM` references for the increment. |
| `@modifydate(ElementName,year,month,day)` | Replace date fields individually; `*` preserves the original. Accepts `@PARAM` references. |

### 3.6  Identifiers

| Function | Behavior |
|----------|----------|
| `@integer(ElementName,KeyType,width)` | Return a sequential integer (starting at 1) for each unique value of *ElementName* within the *KeyType* stream. Zero-padded to *width*. |
| `@initials(ElementName)` | Extract initials from a `Last^First^Middle` name, return as `FML`. |
| `@initials(ElementName,offset)` | Same, then apply Caesar cipher with *offset* (shifts within character groups: lowercase, uppercase, numeric). |

### 3.7  Lookup

| Function | Behavior |
|----------|----------|
| `@lookup(ElementName,KeyType)` | Look up `KeyType/value` in the lookup table properties file.  Quarantines the object if no match is found. |
| `@lookup(ElementName,KeyType,action)` | Same, with a failure action instead of quarantine: |

Failure actions for `@lookup`:

| Action | Behavior on miss |
|--------|------------------|
| `remove` | Delete the element |
| `keep` | Leave the element unmodified |
| `empty` | Replace with empty string |
| `skip` | Abort anonymization, pass the object through unmodified |
| `default` (+ 4th arg) | Use the 4th argument as the replacement value |
| `ignore` (+ 4th arg regex) | Use the original value if it matches the regex |

The *ElementName* argument may be a pipe-separated list
(`PatientID|StudyDate`) to create composite lookup keys
(`KeyType/value1|value2`).

Lookup table values starting with `@` followed by `/` trigger recursive
indirection (max 10 levels).

### 3.8  Encryption

| Function | Behavior |
|----------|----------|
| `@encrypt(ElementName,"key")` | Blowfish-encrypt the value with the given key, return base64. |
| `@encrypt(ElementName,@ParameterName)` | Same, using a parameter value as the key. |

### 3.9  Parameters and extension

| Function | Behavior |
|----------|----------|
| `@param(@ParameterName)` | Return the value of the named parameter. |
| `@call(pluginID,args...)` | Invoke an anonymizer extension registered as a CTP plugin. |
| `@round(ElementName,groupsize)` | Bin values into groups centered at zero (e.g. age 57, groupsize 10 → 60). Preserves trailing alphabetic suffixes. |

### 3.10  Element creation and appending

| Function | Behavior |
|----------|----------|
| `@always()` | Force execution even if the element is absent; creates the element.  The remainder of the script after `@always()` is executed normally. Example: `@always()@hash(this,10)`, `@always()YES`. |
| `@append(){script}` | Append the brace-enclosed script result to a multi-valued element.  Multiple values can be separated with `\\\\` inside the braces. |

### 3.11  Conditionals

Syntax: `@if(ElementName,condition){true clause}{false clause}`

**Both clauses are required.**  Nested `@if` is not supported.  Function calls
are allowed within clauses.

| Condition | True when… |
|-----------|------------|
| `exists` | The element exists in the dataset (regardless of value) |
| `isblank` | The element is absent, has a zero-length value, or contains only whitespace |
| `equals,"string"` | The value exactly equals *string* (case-insensitive) |
| `contains,"string"` | The value contains *string* (case-insensitive) |
| `matches,"regex"` | The value matches the regular expression |
| `greaterthan,value` | The value is numerically greater (non-numeric characters are stripped before comparison) |

Special functions that may only appear within `@if` clauses:

| Function | Behavior |
|----------|----------|
| `@quarantine()` | Abort anonymization and quarantine the unmodified object. |
| `@skip()` | Abort anonymization and pass the unmodified object through the pipeline. |

### 3.12  Sequence processing

| Function | Behavior |
|----------|----------|
| `@process()` | Anonymize all item datasets within a sequence element using the same script. |
| `@select(){root clause}{item clause}` | Execute the first clause when processing the root dataset and the second clause when processing an SQ item dataset. Both clauses required. |

When processing item datasets:
- Element references are limited to the item dataset being processed.
- New elements are **not** created (prevents accidentally adding root-level
  elements into items).
- All parameters remain available.
- `root:ElementName` may be used to reference the root dataset.

### 3.13  DeIdentificationMethodCodeSeq (0012,0064) — special handling

This element is always processed even if absent (unlike other elements, which
require `@always()`).  The `@always()` function is prohibited for this tag.

The action script contains one or more codes from CID 7050
(De-identification Method), separated by `/`:

```
113100/113105/113107/113108/113109
```

CTP builds a proper SQ with one item per code.  Each item contains:

| Tag | Value |
|-----|-------|
| (0008,0100) CodeValue | The code (e.g. `113100`) |
| (0008,0102) CodingSchemeDesignator | `DCM` |
| (0008,0104) CodeMeaning | Looked up from CID 7050 |

The special code `RESET` at the start clears any pre-existing sequence items
before adding the new codes.

CID 7050 codes:

| Code | Meaning |
|------|---------|
| 113100 | Basic Application Confidentiality Profile |
| 113101 | Clean Pixel Data Option |
| 113102 | Clean Recognizable Visual Features Option |
| 113103 | Clean Graphics Option |
| 113104 | Clean Structured Content Option |
| 113105 | Clean Descriptors Option |
| 113106 | Retain Longitudinal With Full Dates Option |
| 113107 | Retain Longitudinal With Modified Dates Option |
| 113108 | Retain Patient Characteristics Option |
| 113109 | Retain Device Identity Option |
| 113110 | Retain UIDs |
| 113111 | Retain Safe Private Option |

### 3.14  Escape characters

The backslash `\` forces the next character to be taken literally.  A literal
backslash requires `\\`.  Inside function arguments, commas, parentheses, and
brackets must be escaped if they are part of the argument text.  Inside regex
strings, double-escaping is required (`\\\\d` produces `\d` for the regex
engine).

---

## 4  DICOM Pixel Anonymizer Script

The pixel anonymizer script is a plain-text file (not XML) that defines
regions of pixels to black out based on DICOM header conditions.

### 4.1  Structure

The file consists of:
- **Comment text**: any text not inside `{ }` or `( )` is treated as a
  comment.  Comments may not contain braces or parentheses.
- **Condition blocks**: expressions enclosed in `{ }` that evaluate DICOM
  headers.
- **Region coordinates**: one or more `(x, y, width, height)` tuples
  following a condition block.

A condition block and its following region coordinates form one rule.
Multiple rules may appear in the file.

```
GE CT Dose Series
  { [0008,0060].containsIgnoreCase("CT") *
    [0008,0070].containsIgnoreCase("GE MEDICAL") *
    [0008,103e].containsIgnoreCase("Dose Report") }
  (0,0,512,110)
```

### 4.2  Condition expressions

Conditions inside `{ }` use the same expression language as the DICOM Filter
(section 5).  They evaluate to a boolean: when true, the associated pixel
regions are masked.

### 4.3  Region coordinates

Each region is specified as `(x, y, width, height)` where:

| Field | Meaning |
|-------|---------|
| `x` | Horizontal offset of the left edge (pixels) |
| `y` | Vertical offset of the top edge (pixels) |
| `width` | Width of the region (pixels) |
| `height` | Height of the region (pixels) |

- Negative `x`: the region is offset inward from the right edge.
- Negative `y`: the region is offset inward from the bottom edge.
- Regions are clipped to the image bounds.

Multiple coordinate tuples may follow a single condition block:

```
  { Modality.equals("US") * Manufacturer.containsIgnoreCase("ATL") }
  (0,0,240,56) (0,730,240,24)
```

---

## 5  DICOM Filter Script

The filter script is a boolean expression that determines whether a DICOM
object should be accepted (expression is true) or rejected/quarantined
(expression is false).

### 5.1  Predicates

Predicates operate on DICOM element values.  The syntax is
`identifier.method("argument")`.

| Method | True when… |
|--------|------------|
| `equals("str")` | Value exactly equals *str* |
| `equalsIgnoreCase("str")` | Case-insensitive equality |
| `contains("str")` | Value contains *str* |
| `containsIgnoreCase("str")` | Case-insensitive containment |
| `startsWith("str")` | Value starts with *str* |
| `startsWithIgnoreCase("str")` | Case-insensitive prefix |
| `endsWith("str")` | Value ends with *str* |
| `endsWithIgnoreCase("str")` | Case-insensitive suffix |
| `matches("regex")` | Value matches the regular expression |
| `isLessThan("value")` | Numeric comparison (non-numeric characters stripped, compared as float) |
| `isGreaterThan("value")` | Numeric comparison |

### 5.2  Identifiers

| Format | Example |
|--------|---------|
| DICOM keyword | `Modality`, `Manufacturer`, `Rows` |
| Bracketed hex | `[0008,0060]`, `[00280010]` |
| Sequence navigation | `SeqOfUltrasoundRegions::RegionLocationMinY0` |
| Private tag | `[0029,1140]` or `[0029[XYZ CT HEADER]40]` |

A missing element evaluates as an empty string `""`.

### 5.3  Operators

Listed from lowest to highest precedence:

| Operator | Meaning |
|----------|---------|
| `+` | Logical OR |
| `*` | Logical AND |
| `!` | Logical NOT (unary prefix) |
| `( )` | Grouping |

### 5.4  Comments

Text from `//` to the end of the line is a comment.

### 5.5  Example

```
![0008,0008].contains("SAVE") *
![0008,103e].contains("SAVE") *
![0028,0301].contains("YES")
```

This filter accepts objects where ImageType does not contain "SAVE", the
series description does not contain "SAVE", and BurnedInAnnotation does not
contain "YES".

A more complex example using OR and grouping:

```
(
  (Manufacturer.containsIgnoreCase("KONICA") * Modality.startsWithIgnoreCase("CR"))
  +
  (Manufacturer.containsIgnoreCase("SIEMENS") * Modality.equals("CT"))
)
```

---

## 6  Translation gaps

The following CTP features are **not** expressible in the dicom-deid-rs recipe
format and are omitted or approximated during translation:

| CTP Feature | Status | Reason |
|---|---|---|
| `@encrypt()` | Skipped | Excluded per project decision |
| `@call()` | Skipped | Plugin-specific, no equivalent |
| `@select(){root}{item}` | Comment emitted | Root-vs-item context distinction not yet modeled |
| `@dateinterval()` | Not implemented | Not used in any shipped script |
| `@pathelement()` | Not implemented | Not used in any shipped script |
| `@initials(Element,offset)` Caesar cipher | Not implemented | Only the base `@initials()` is supported |
| `@lookup()` composite keys (`PatientID\|StudyDate`) | Not implemented | Rare usage |
| `@lookup()` recursive indirection (`@/`) | Not implemented | Rare, complex |
| Element addressing: sequence navigation (`::`) | Not implemented | Not used in shipped scripts |
| Element addressing: `root:` prefix | Not implemented | Only relevant inside `@select()` |
| Filter: `endsWith` / `endsWithIgnoreCase` | Approximated as `contains` | No suffix match in recipe format |
| Filter: `matches` (regex) | Approximated as `contains` | Recipe predicates don't support regex |
| Filter: `isLessThan` / `isGreaterThan` | Silently skipped | No numeric comparison in recipe format |

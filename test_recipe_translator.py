"""Tests for recipe_translator.py -- CTP script to recipe format translation."""

from recipe_translator import (
    build_recipe,
    translate_anonymizer_script,
    translate_filter_script,
    translate_pixel_anonymizer_script,
)


# ---------------------------------------------------------------------------
# Anonymizer script tests
# ---------------------------------------------------------------------------


class TestTranslateAnonymizerScript:
    def test_keep_action(self):
        xml = '<script><e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "KEEP (0008,0005)" in lines

    def test_remove_action(self):
        xml = (
            '<script><e en="T" t="00080080" n="InstitutionName">@remove()</e></script>'
        )
        lines, _ = translate_anonymizer_script(xml)
        assert "REMOVE (0008,0080)" in lines

    def test_empty_action(self):
        xml = '<script><e en="T" t="00080090" n="ReferringPhysicianName">@empty()</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "BLANK (0008,0090)" in lines

    def test_blank_for_empty_text(self):
        xml = '<script><e en="T" t="00100030" n="PatientBirthDate"></e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "BLANK (0010,0030)" in lines

    def test_hashuid_action(self):
        xml = '<script><e en="T" t="0020000D" n="StudyInstanceUID">@hashuid(@UIDROOT,this)</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "REPLACE (0020,000D) func:hashuid" in lines

    def test_hash_action(self):
        xml = '<script><e en="T" t="00100020" n="PatientID">@hash(this,10)</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "REPLACE (0010,0020) func:hash" in lines

    def test_hashname_action(self):
        xml = '<script><e en="T" t="00100010" n="PatientName">@hashname(this,6,2)</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "REPLACE (0010,0010) func:hashname" in lines

    def test_incrementdate_action(self):
        xml = """<script>
            <p t="DATEINC">-3210</p>
            <e en="T" t="00080020" n="StudyDate">@incrementdate(this,@DATEINC)</e>
        </script>"""
        lines, params = translate_anonymizer_script(xml)
        assert "JITTER (0008,0020) var:DATEINC" in lines
        assert params["DATEINC"] == "-3210"

    def test_param_action(self):
        xml = """<script>
            <p t="SITEID">1</p>
            <e en="T" t="00120010" n="ClinicalTrialSponsorName">@param(@SITEID)</e>
        </script>"""
        lines, params = translate_anonymizer_script(xml)
        assert "REPLACE (0012,0010) var:SITEID" in lines
        assert params["SITEID"] == "1"

    def test_always_action(self):
        xml = '<script><e en="T" t="00120062" n="PatientIdentityRemoved">@always()YES</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "ADD (0012,0062) YES" in lines

    def test_lookup_action(self):
        xml = '<script><e en="T" t="00100020" n="PatientID">@lookup(this,PatientID,keep)</e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert "REPLACE (0010,0020) func:lookup" in lines

    def test_disabled_elements_skipped(self):
        xml = """<script>
            <e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e>
            <e en="F" t="00080008" n="ImageType"></e>
            <e en="F" t="00080060" n="Modality">@keep()</e>
        </script>"""
        lines, _ = translate_anonymizer_script(xml)
        assert any("0008,0005" in line for line in lines)
        assert not any("0008,0008" in line for line in lines)
        assert not any("0008,0060" in line for line in lines)

    def test_removal_rules(self):
        xml = """<script>
            <r en="T" t="curves">Remove curves</r>
            <r en="T" t="overlays">Remove overlays</r>
            <r en="F" t="privategroups">Remove private groups</r>
        </script>"""
        lines, _ = translate_anonymizer_script(xml)
        assert any("REMOVE (5000-501e,*)" in line for line in lines)
        assert any("REMOVE (6000-601e,*)" in line for line in lines)
        assert not any("private" in line for line in lines)

    def test_hex_tag_fallback(self):
        xml = '<script><e en="T" t="00091001"></e></script>'
        lines, _ = translate_anonymizer_script(xml)
        assert any("(0009,1001)" in line for line in lines)

    def test_full_script(self):
        xml = """<script>
            <p t="DATEINC">-3210</p>
            <p t="UIDROOT">1.2.840.113654.2.70.1</p>
            <e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e>
            <e en="T" t="00080080" n="InstitutionName">@remove()</e>
            <e en="T" t="00080090" n="ReferringPhysicianName">@empty()</e>
            <e en="T" t="0020000D" n="StudyInstanceUID">@hashuid(@UIDROOT,this)</e>
            <e en="T" t="00080020" n="StudyDate">@incrementdate(this,@DATEINC)</e>
            <e en="T" t="00100020" n="PatientID">@hash(this,10)</e>
            <e en="T" t="00100010" n="PatientName">@hashname(this,6,2)</e>
        </script>"""
        lines, params = translate_anonymizer_script(xml)
        assert len(lines) == 7
        assert params["DATEINC"] == "-3210"
        assert params["UIDROOT"] == "1.2.840.113654.2.70.1"


# ---------------------------------------------------------------------------
# Pixel anonymizer script tests
# ---------------------------------------------------------------------------


class TestTranslatePixelAnonymizerScript:
    def test_basic_conditions_and_coordinates(self):
        script = """
CT Dose Series
  { [0008,0104].containsIgnoreCase("IEC Body Dosimetry Phantom") }
  (0,0,512,200)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "LABEL CT Dose Series" in result
        assert "contains 00080104 IEC Body Dosimetry Phantom" in result
        assert "ctpcoordinates 0,0,512,200" in result

    def test_and_or_operators(self):
        script = """
Test Block
  { Manufacturer.containsIgnoreCase("GE MEDICAL") * SeriesDescription.containsIgnoreCase("Dose Report") + Modality.equals("CT") }
  (0,0,512,110)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "contains Manufacturer GE MEDICAL" in result
        assert "+ contains SeriesDescription Dose Report" in result
        assert "|| equals Modality CT" in result

    def test_negation(self):
        script = """
Negation Test
  { !Manufacturer.containsIgnoreCase("ACME") * !Modality.equals("OT") }
  (0,0,100,100)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "notcontains Manufacturer ACME" in result
        assert "notequals Modality OT" in result

    def test_hex_tag_refs(self):
        script = """
Hex Test
  { [0008,0070].containsIgnoreCase("Philips") * [0028,0010].equals("446") }
  (0,0,125,50)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "contains 00080070 Philips" in result
        assert "equals 00280010 446" in result

    def test_startswith_predicate(self):
        script = """
StartsWith Test
  { [0018,1020].startsWithIgnoreCase("V6") }
  (0,0,100,50)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "startswith 00181020 V6" in result

    def test_multiple_blocks(self):
        script = """
Block One
  { Manufacturer.containsIgnoreCase("GE") }
  (0,0,512,100)

Block Two
  { Manufacturer.containsIgnoreCase("SIEMENS") }
  (0,0,1024,60)
        """
        lines = translate_pixel_anonymizer_script(script)
        result = "\n".join(lines)
        assert "LABEL Block One" in result
        assert "LABEL Block Two" in result


# ---------------------------------------------------------------------------
# Filter script tests
# ---------------------------------------------------------------------------


class TestTranslateFilterScript:
    def test_empty_filter(self):
        assert translate_filter_script("") == []
        assert translate_filter_script("true.") == []

    def test_simple_filter(self):
        script = 'Modality.containsIgnoreCase("CT")'
        lines = translate_filter_script(script)
        result = "\n".join(lines)
        assert "LABEL filter_rule_0" in result
        assert "contains Modality CT" in result

    def test_and_filter(self):
        script = (
            'Modality.containsIgnoreCase("CT") * Manufacturer.containsIgnoreCase("GE")'
        )
        lines = translate_filter_script(script)
        result = "\n".join(lines)
        assert "contains Modality CT" in result
        assert "+ contains Manufacturer GE" in result

    def test_or_filter_creates_separate_labels(self):
        script = (
            '(Modality.containsIgnoreCase("CT")) + (Modality.containsIgnoreCase("MR"))'
        )
        lines = translate_filter_script(script)
        result = "\n".join(lines)
        assert "LABEL filter_rule_0" in result
        assert "LABEL filter_rule_1" in result

    def test_startswith_filter(self):
        script = '[0018,1020].startsWithIgnoreCase("V6")'
        lines = translate_filter_script(script)
        result = "\n".join(lines)
        assert "startswith 00181020 V6" in result


# ---------------------------------------------------------------------------
# Build recipe tests
# ---------------------------------------------------------------------------


class TestBuildRecipe:
    def test_combined_output(self):
        anonymizer_xml = """<script>
            <e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e>
            <e en="T" t="00080080" n="InstitutionName">@remove()</e>
        </script>"""
        pixel_script = """
Test Label
  { Manufacturer.containsIgnoreCase("GE") }
  (0,0,512,100)
        """
        recipe, variables = build_recipe(
            anonymizer_xml=anonymizer_xml, pixel_script=pixel_script
        )

        assert recipe.startswith("FORMAT dicom")
        assert "%filter graylist" in recipe
        assert "LABEL" in recipe
        assert "%header" in recipe
        assert "KEEP (0008,0005)" in recipe
        assert "REMOVE (0008,0080)" in recipe

    def test_anonymizer_only(self):
        xml = '<script><e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e></script>'
        recipe, _ = build_recipe(anonymizer_xml=xml)
        assert "%header" in recipe
        assert "%filter" not in recipe

    def test_pixel_only(self):
        script = """
Test
  { Modality.equals("CT") }
  (0,0,512,100)
        """
        recipe, _ = build_recipe(pixel_script=script)
        assert "%filter graylist" in recipe
        assert "%header" not in recipe

    def test_variables_extracted(self):
        xml = """<script>
            <p t="DATEINC">-3210</p>
            <e en="T" t="00080020" n="StudyDate">@incrementdate(this,@DATEINC)</e>
        </script>"""
        _, variables = build_recipe(anonymizer_xml=xml)
        assert variables["DATEINC"] == "-3210"

    def test_with_filter(self):
        filter_script = 'Modality.containsIgnoreCase("CT")'
        recipe, _ = build_recipe(filter_script=filter_script)
        assert "%filter whitelist" in recipe
        assert "contains Modality CT" in recipe

    def test_empty_recipe(self):
        recipe, variables = build_recipe()
        assert recipe.startswith("FORMAT dicom")
        assert variables == {}

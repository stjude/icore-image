<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="xml" encoding="utf-8" omit-xml-declaration="yes" />

<xsl:param name="home"/>

<xsl:template match="/Files">
	<html>
		<head>
			<title>Files</title>
			<link rel="Stylesheet" type="text/css" media="all" href="/BaseStyles.css"></link>
			<link rel="Stylesheet" type="text/css" media="all" href="/FileServerServlet.css"></link>
			<script> var home = '<xsl:value-of select="$home"/>';</script>
		</head>
		<body>
			<div class="closebox">
				<xsl:if test="$home">
					<img src="/icons/home.png"
						 onclick="window.open('{$home}','_self');"
						 title="Return to the home page"/>
					<br/>
				</xsl:if>
			</div>

			<h1>Files</h1>
			<xsl:apply-templates select="Directory"/>
			<br/><br/>
		</body>
	</html>
</xsl:template>

<xsl:template match="Directory">
	<xsl:variable name="files" select="File"/>
	<xsl:if test="$files">
		<h2><xsl:value-of select="@url"/></h2>
		<table>
			<xsl:for-each select="File">
				<tr>
					<td width="80%">
						<a href="{@url}"><xsl:value-of select="@name"/></a>
					</td>
					<td class="right">
						<xsl:value-of select="@size"/>
					</td>
				</tr>
			</xsl:for-each>
		</table>
	</xsl:if>
	<xsl:apply-templates select="Directory"/>
</xsl:template>

</xsl:stylesheet>
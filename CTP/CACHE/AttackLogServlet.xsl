<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="xml" encoding="utf-8" omit-xml-declaration="yes" />

<xsl:param name="home"/>

<xsl:template match="/Attackers">
	<html>
		<head>
			<title>Attack Log</title>
			<link rel="Stylesheet" type="text/css" media="all" href="/BaseStyles.css"></link>
			<link rel="Stylesheet" type="text/css" media="all" href="/AttackLogServlet.css"></link>
			<script> var home = '<xsl:value-of select="$home"/>';</script>
			<script language="JavaScript" type="text/javascript" src="/JSUtil.js">;</script>
		</head>
		<body>
		<xsl:if test="$home">
			<div class="closebox">
				<img src="/icons/home.png"
					 onclick="window.open('{$home}','_self');"
					 title="Return to the home page"/>
				<br/>
			</div>
		</xsl:if>

		<h1>Attack Log</h1>

		<center>
			<xsl:if test="Attacker">
				<table border="1">
					<tr>
						<th>Attacker</th>
						<th>City</th>
						<th>Region</th>
						<th>Country</th>
						<th class="count">Count</th>
						<th>Last Attack</th>
					</tr>
					<xsl:for-each select="Attacker">
						<tr>
							<td>
								<a href="http://gd.geobytes.com/GetCityDetails?fqcn={@ip}"
								   target="geobytes">
									<xsl:value-of select="@ip"/>
								</a>
							</td>
							<td><xsl:value-of select="@city"/></td>
							<td><xsl:value-of select="@region"/></td>
							<td><xsl:value-of select="@country"/></td>
							<td class="count"><xsl:value-of select="@count"/></td>
							<td><xsl:value-of select="@last"/></td>
						</tr>
					</xsl:for-each>
				</table>
			</xsl:if>
			<xsl:if test="not(Attacker)">
				<p>The Attack Log is empty.</p>
			</xsl:if>
		</center>

		</body>
	</html>
</xsl:template>

</xsl:stylesheet>
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="xml" encoding="utf-8" omit-xml-declaration="yes" />

<xsl:param name="home"/>

<xsl:template match="/Classes">
	<html>
		<head>
			<title>Set Logger Level</title>
			<link rel="Stylesheet" type="text/css" media="all" href="/BaseStyles.css"></link>
			<link rel="Stylesheet" type="text/css" media="all" href="/LoggerLevelServlet.css"></link>
			<script> var home = '<xsl:value-of select="$home"/>';</script>
			<script language="JavaScript" type="text/javascript" src="/JSUtil.js">;</script>
			<script language="JavaScript" type="text/javascript" src="/LoggerLevelServlet.js">;</script>
		</head>
		<body>
		<div class="closebox">
			<xsl:if test="$home">
				<img src="/icons/home.png"
					 onclick="window.open('{$home}','_self');"
					 title="Return to the home page"/>
				<br/>
			</xsl:if>
			<img src="/icons/save.png"
				 onclick="save();"
				 title="Set the logger level"/>
		</div>

		<h1>Set Logger Level</h1>

		<form id="formID" method="post" action="" accept-charset="UTF-8">
		<xsl:if test="not($home)">
			<input type="hidden" name="suppress" value=""/>
		</xsl:if>

		<p class="note">
			This page can be used to set the logger level for classes in the application.
			It should be used only when directed by a technical support engineer.
			Setting the logger level below INFO can result in the application log growing
			very rapidly.
		</p>

		<p class="note">
			Select the class from the pull-down field or enter
			the fully qualified class name of the class in the
			text field. Then select the logger level for the
			selected class.</p>
		<center>
			<table border="1">
				<tr>
					<td width="30%">Class name</td>
					<td width="70%">
						<select id="ClassSelector" onchange="setClass()">
							<option value=""/>
							<xsl:for-each select="Class">
								<xsl:sort select="@name"/>
								<option value="{@path}"><xsl:value-of select="@name"/></option>
							</xsl:for-each>
						</select>
						<br/>
						<input class="text" type="text" id="class" name="class"/>
					</td>
				</tr>
				<tr>
					<td>Level</td>
					<td>
						<input type="radio" name="level" value="OFF" id="OFF">
							<label for="OFF">OFF</label>
						</input><br/>
						<input type="radio" name="level" value="ERROR" id="ERROR">
							<label for="ERROR">ERROR</label>
						</input><br/>
						<input type="radio" name="level" value="WARN" id="WARN">
							<label for="WARN">WARN</label>
						</input><br/>
						<input type="radio" name="level" value="INFO" id="INFO">
							<label for="INFO">INFO</label>
						</input><br/>
						<input type="radio" name="level" value="DEBUG" id="DEBUG" checked="true">
							<label for="DEBUG">DEBUG</label>
						</input><br/>
					</td>
				</tr>
			</table>
		</center>

		</form>

		</body>
	</html>
</xsl:template>

</xsl:stylesheet>
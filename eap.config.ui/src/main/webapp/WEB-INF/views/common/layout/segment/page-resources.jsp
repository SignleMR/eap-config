<%@page import="eap.util.JsonUtil"%>
<%@page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" trimDirectiveWhitespaces="true" %>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script type="text/javascript">
	var contextPath = '<%=request.getContextPath()%>', formTokenField = '<%=env.getFormTokenField()%>', urls = <%=JsonUtil.toJson(env.getDomainUrls())%>;
</script>
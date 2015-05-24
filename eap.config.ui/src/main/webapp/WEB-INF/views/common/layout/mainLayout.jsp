<%@page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" trimDirectiveWhitespaces="true" %>
<%@include file="/WEB-INF/views/common/jsp-header.jsp"%>
<!DOCTYPE html>
<html lang="zh-cn">
<head>
<title><decorator:title>配置管理中心</decorator:title></title>
<%@include file="segment/page-resources.jsp"%>
<link rel="stylesheet" href="<e:url value='/static/lib/bootstrap-3.2.0/css/bootstrap.min.css' />">
<link rel="stylesheet" href="<e:url value='/static/lib/bootstrap-3.2.0/css/bootstrap-theme.min.css' />">
<script src="<e:url value='/static/lib/jquery-1.11.1/jquery-1.11.1.min.js' />"></script>
<decorator:head />
</head>
<body class="main" >
<%@include file="segment/page-header.jsp" %>
<decorator:body />
<%@include file="segment/page-footer.jsp" %>
<%@include file="/WEB-INF/views/common/jsp-footer.jsp"%>
</body>
</html>
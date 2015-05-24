<%@page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" trimDirectiveWhitespaces="true" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@taglib prefix="decorator" uri="http://www.opensymphony.com/sitemesh/decorator" %>
<%@taglib prefix="page" uri="http://www.opensymphony.com/sitemesh/page" %>
<%@taglib prefix="e" uri="eap/web/tags" %>
<%@page import="eap.Env" %>
<%@page import="eap.EapContext" %>
<%@page import="eap.WebEnv" %>
<%@page import="eap.config.module.P"%>
<%WebEnv env = (WebEnv) EapContext.getEnv(); %>
<c:set var="env" value="<%=env %>" scope="request" />
<e:useConstants var="Env" className="eap.WebEnv" />
<e:useConstants var="P" className="eap.config.module.P" />
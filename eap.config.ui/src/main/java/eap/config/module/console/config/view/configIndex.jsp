<%@page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8" trimDirectiveWhitespaces="true" %>
<%@include file="/WEB-INF/views/common/jsp-header.jsp"%>
<!DOCTYPE html">
<html lang="zh-cn">
<head>
<style type="text/css">
body {
	padding-top: 50px;
}
.table-hover > tbody > .expand-row:hover > td, .table-hover > tbody > .expand-row:hover > th {
	background-color: #FFF;
}
.table>thead>tr>th, .table>tbody>tr>th, .table>tfoot>tr>th, .table>thead>tr>td, .table>tbody>tr>td, .table>tfoot>tr>td {
	vertical-align: middle;
}

.workspace .wb {
	word-break: break-all;
}
.workspace .row {
	margin: 0;
	padding: 5px;
}
#current-app {
	color: #FFF;
}
.nav-item-label {
	padding-top: 15px;
	padding-bottom: 15px;
	line-height: 20px;
	color: #777;
}

.app-detail-panel .panel-body {
	padding: 0px;
}
.app-config-panel .panel-body .table {
	margin-bottom: 0px;
}

.app-version-list {
	border-radius: 0px;
	-webkit-box-shadow: none;
	box-shadow: none;
}
.app-version-list .list-group-item {
	border: 0px;
}
.app-version-list .list-group-item:first-child, 
.app-version-list .list-group-item:last-child {
	border-radius: 0px;
}
.app-version-list .list-group-item.active {
	z-index: 2;
	color: #555;
	background-color: #f5f5f5;
	border-color: #f5f5f5;
	background-image: none;
}

.expand-row > td {
	border: none !important;
}
.expand-row > td > pre {
	border: none;
}

.app-list-panel {
	border-right: 1px solid #ddd;
}

.app-detail-panel .cli-form {
	background-color: #FFF;
	position: fixed;
	bottom: 15px;
	z-index: 3;
}

.app-list {
	border-radius: 0px;
	-webkit-box-shadow: none;
	box-shadow: none;
}
.app-list .list-group-item {
	border: 0px;
}
.app-list .list-group-item:first-child, 
.app-list .list-group-item:last-child {
	border-radius: 0px;
}

</style>
</head>
<body>
<header class="navbar navbar-inverse navbar-fixed-top gbc-navbar" role="banner">
	<div class="container">
		<div class="navbar-header">
			<div class="navbar-brand">配置管理中心</div>
		</div>
		<nav class="collapse navbar-collapse" role="navigation">
			<ul class="nav navbar-nav ">
				<li><div class="nav-item-label">Zookeeper -> </div></li>
				<li class="dropdown">
					<a href="#" role="button" class="dropdown-toggle" data-toggle="dropdown"><span id="current-cm-server">${firstCmServers }</span><b class="caret"></b></a>
					<ul id="cm-server-select" class="dropdown-menu" role="menu">
						<c:forEach var="cmServer" items="${cmServers }">
							<li role="presentation"><a role="menuitem" tabindex="-1" href="#">${cmServer }</a></li>
						</c:forEach>
					</ul>
				</li>
			</ul>
			<ul class="nav navbar-nav navbar-right">
				<li><a class="glyphiconlink nav-item-label" href="#" title="消息"><span class="glyphicon glyphicon-envelope"></span></a></li>
			</ul>
		</nav>
	</div>
</header>
<div class="workspace">
	<div class="row">
		<div class="col-md-3 app-list-panel">
			<h4>应用列表</h4>
			<div id="app-list-box" class="list-group app-list">
			</div>
			<div style="display:none">
				<form id="add-app-form" class="form-horizontal" role="form">
					<input type="text" class="form-control" id="appName" name="appName" placeholder="应用名称">
				</form>
			</div>
			<div><button id="add-app-btn" class="btn btn-default btn-sm">添加应用</button></div>
			<script id="app-list-item-tpl" type="text/x-jquery-tmpl">
				<a href="#" class="list-group-item ${'$'}{cssClass}" data-id="${'$'}{appName}"><span class="badge">${'$'}{serverCount}</span>${'$'}{appName}</a>
			</script>
		</div>
		<div class="col-md-9 app-detail-panel">
			<ul class="nav nav-tabs" role="tablist">
				<li class="active"><a href="#app-runtime-tab-content" role="tab" data-toggle="tab" data-id="runtime">运行时</a></li>
				<li><a href="#app-config-tab-content" role="tab" data-toggle="tab" data-id="config">配置</a></li>
				<li><a href="#app-cli-tab-content" role="tab" data-toggle="tab" data-id="cli">命令行</a></li>
			</ul>
			<div class="tab-content">
				<div class="tab-pane active" id="app-runtime-tab-content">
					<table class="table table-hover">
						<thead>
							<th>#</th>
							<th>集群节点</th>
							<th>应用编号</th>
							<th>应用版本</th>
							<th>启动时间</th>
						</thead>
						<tbody id="runtime-app-details-list-box">
						</tbody>
						<script id="runtime-app-details-list-item-tpl" type="text/x-jquery-tmpl">
							<tr data-id="${'$'}{server}" data-app-version="${'$'}{appVersion}" class="${'$'}{cssClass}">
								<td>${'$'}{index}</td>
								<td><a href="http://${'$'}{server}/console/druid/index.html" target="_blank" title="查看Druid监控">${'$'}{server}</a></td>
								<td>${'$'}{appId}</td>
								<td>${'$'}{appVersion}</td>
								<td>${'$'}{startTime}</td>
							</tr>
						</script>
					</table>
					<div class="panel panel-default app-config-panel">
						<div class="panel-heading">应用配置</div>
						<div class="panel-body">
							<table class="table">
								<tbody id="runtime-app-config-list-box">
								</tbody>
								<script id="runtime-app-config-list-item-tpl" type="text/x-jquery-tmpl">
									<tr>
										<td width="100px">${'$'}{key}</td>
										<td class="wb">${'$'}{value}</td>
									</tr>
								</script>
							</table>
						</div>
					</div>
					<div class="panel panel-default">
						<div class="panel-heading">Java虚拟机</div>
						<div class="panel-body">
						</div>
					</div>
					<div id="app-container-panel" class="panel panel-default">
						<div class="panel-heading">应用容器</div>
						<div class="panel-body">
						</div>
					</div>
				</div>
				<div class="tab-pane fade" id="app-config-tab-content">
					<div class="row">
						<div class="col-md-2">
							<h5>配置版本</h5>
							<div id="app-version-list-box" class="list-group app-version-list">
							</div>
							<script id="app-version-list-item-tpl" type="text/x-jquery-tmpl">
								<a href="#" class="list-group-item ${'$'}{cssClass}" data-id="${'$'}{version}">${'$'}{version}</a>
							</script>
							<div style="display:none">
								<form id="add-version-form" class="form-horizontal" role="form">
									<input type="text" class="form-control" id="appVersion" name="appVersion" placeholder="版本号">
								</form>
							</div>
							<div><button id="add-version-btn" class="btn btn-default btn-sm">添加版本</button></div>
						</div>
						<div class="col-md-10">
							<table class="table table-hover">
								<thead>
									<th width="100px">键</th>
									<th>值</th>
									<th width="120px">&nbsp;</th>
								</thead>
								<tbody id="app-config-list-box">
								</tbody>
								<script id="app-config-list-item-tpl" type="text/x-jquery-tmpl">
									<tr>
										<td>${'$'}{key}</td>
										<td class="wb">${'$'}{value}</td>
										<td>
											<div class="btn-toolbar" role="toolbar">
												<div class="btn-group btn-group-sm">
													<button type="button" class="btn btn-info modify-config-btn">修改</button>
													<button type="button" class="btn btn-danger delete-config-btn">删除</button>
												</div>
											</div>
										</td>
									</tr>
								</script>
							</table>
							<div style="display:none">
								<form id="add-config-form" class="form-horizontal" role="form">
									<input type="text" class="form-control" id="configAttrKey" name="configAttrKey" placeholder="键">
									<textarea rows="15" class="form-control" style="margin-top: 5px" id="configAttrValue" name="configAttrValue" placeholder="值"></textarea>
								</form>
							</div>
							<div><button id="add-config-btn" class="btn btn-default btn-sm">添加配置</button></div>
						</div>
					</div>
				</div>
				<div class="tab-pane fade" id="app-cli-tab-content" style="margin-bottom: 30px;">
					<div class="row">
						<table class="table table-hover">
							<thead>
								<th>#</th>
								<th><input type="checkbox" id="cli-app-list-selector" /></th>
								<th>集群节点</th>
								<th>应用编号</th>
								<th>应用版本</th>
								<th>命令执行状态</th>
								<th>&nbsp;</th>
							</thead>
							<tbody id="cli-app-list-box">
							</tbody>
							<script id="cli-app-list-item-tpl" type="text/x-jquery-tmpl">
								<tr>
									<td>${'$'}{index}</td>
									<td><input type="checkbox" class="server-selector" value="${'$'}{server}" ${'$'}{selected} /></td>
									<td>${'$'}{server}</td>
									<td>${'$'}{appId}</td>
									<td>${'$'}{appVersion}</td>
									<td>
										<div class="progress" style="margin-bottom: 0px;" title="${'$'}{cliExecStatus}">
											<div class="progress-bar ${'$'}{cliExecStatusCssClass}" role="progressbar" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100" style="width: 100%">
												<span class="sr-only">${'$'}{cliExecStatus}</span>
											</div>
										</div>
									</td>
									<td>
										<div class="btn-toolbar" role="toolbar">
											<div class="btn-group btn-group-sm">
												<button class="btn btn-info ${'$'}{expandBtnCssClass} expand-btn" data-id="${'$'}{server}">详细</button>
											</div>
										</div>
									</td>
								</tr>
								<tr class="expand-row" data-id="${'$'}{server}" style="${'$'}{expandRowCssStyle}">
									<td colspan="2">&nbsp;</td>
									<td colspan="5">
										<pre>${'$'}{cliContent}</pre>
									</td>
								</tr>
								</script>
						</table>
					</div>
					<div class="row cli-form">
						<form class="form-horizontal" role="form">
							<div class="col-sm-11">
								<div class="input-group">
									<div class="input-group-addon">命令</div>
									<input id="cli-input" type="text" class="form-control" placeholder='className methodName arg1 \"arg2-1 arg2-2\" arg3' />
								</div>
							</div>
							<div class="col-sm-1">
								<input type="button" id="cli-btn" class="btn btn-default" value="执行" />
							</div>
						</form>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
<script src="<e:url value='/static/lib/bootstrap-3.2.0/js/bootstrap.min.js' />"></script>
<script src="<e:url value='/static/scripts/jquery.bootstrap.min.js' />"></script>
<script src="<e:url value='/static/scripts/jquery.tmpl.min.js' />"></script>
<script type="text/javascript">
	/* ----------- GLOBAL ----------- */
	var SESSION = {
		cmServer:  '${firstCmServers}',
		interval: {
			loadTabPanelTask: 5000,
			loadAppListTask: 5000,
			loadAppConfigListTask: 10000,
			loadRuntimeAppConfigListTask: 10000,
		}
	};
	
	function ajax(options) {
		$.ajax($.extend({
			dataType: 'json',
			type: 'GET',
			cache: false
		}, options));
	}
	
	/* ----------- Timer ----------- */
	var Timer = {
		tasks: {}
	};
	Timer.start = function(taskId, subTaskId, taskFn, interval, delay) {
		if (typeof subTaskId == 'function') {
			delay = interval;
			interval = taskFn;
			taskFn = subTaskId;
			subTaskId = null;
		}
		
		if (delay) {
			setTimeout(taskFn, delay);
		}
		
		this.stop(taskId, subTaskId);
		if (subTaskId) {
			var subTasks = (this['subTasks-' + taskId] || {});
			subTasks[subTaskId] = setInterval(taskFn, interval);
			
			this['subTasks-' + taskId] = subTasks;
		} else {
			this.tasks[taskId] = setInterval(taskFn, interval);
		}
	};
	Timer.stop = function(taskId, subTaskId) {
		if (subTaskId) {
			var subTasks = this['subTasks-' + taskId], 
				subTask = subTasks ? subTasks[subTaskId] : null;
			if (subTask) {
				clearInterval(subTask);
				delete this['subTasks-' + taskId][subTaskId];
			}
		} else {
			var task = this.tasks[taskId];
			if (task) {
				var subTasks = this['subTasks-' + taskId];
				for (var _subTaskId in subTasks) {
					clearInterval(subTasks[_subTaskId]);
				}
				delete this['subTasks-' + taskId];
				
				clearInterval(task);
				this.tasks = {};
			}
		}
	};
	
	/* ---------------------- */
	$(function() {
		$('#cm-server-select').on('click', 'a', function(e) {
			var currCmServer = $(e.currentTarget).text();
			$('#current-cm-server').text(currCmServer);
			SESSION.cmServer = currCmServer;
			
			loadAppList(function() {
				$('.app-detail-panel .nav-tabs a[data-id="runtime"]').trigger('click');
			});
		});
		
		function loadTabPanel(appName, tab) {
			Timer.start('loadTabPanelTask', function() {
				eval('loadTabPanel_' + tab)(appName);
			}, SESSION.interval.loadTabPanelTask, true);
		};
		$('.app-detail-panel .nav-tabs').on('click', 'a', function(e) {
			var appName = SESSION.appName;
			if (appName) {
				var currTab = $(e.currentTarget).attr('data-id');
				SESSION[appName].currTab = currTab;
				
				loadTabPanel(appName, SESSION[appName].currTab);
			}
		});
		
		/* ---------------------- */
		var appListBox = $('#app-list-box');
		appListBox.on('click', 'a', function(e) {
			appListBox.find('.active').removeClass('active');
			
			var item = $(e.currentTarget);
			item.addClass('active');
			
			var appName = item.attr('data-id');
			SESSION.appName = appName;
			SESSION[appName] = {
				currTab: 'runtime',
				runtime: {
					currServer: ''
				},
				config: {
					currVersion: ''
				},
				cli: {
					currServers: {},
					expandRows: {}
				}
			};
			
			$('.app-detail-panel .nav-tabs a[data-id="runtime"]').trigger('click');
			
			return false;
		});
		function loadAppList(callback) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPLIST}" />',
				data: {cmServer: SESSION.cmServer},
				success: function(response) {
					var data = response.result;
					if (SESSION.appName) {
						var notFind = true;
						$.each(data, function(index, item) {
							if (item.appName == SESSION.appName) {
								item.cssClass = 'active';
								notFind = false;
								return false;
							}
						});
						if (notFind) {
							SESSION.appName = '';
						}
					}
					
					appListBox.empty();
					$('#app-list-item-tpl').tmpl(data).appendTo(appListBox);
					
					callback && callback();
				}
			});
		}
		Timer.start('loadAppListTask', loadAppList, SESSION.interval.loadAppListTask, true);
		
		/* ---------- RUNTIME ------------ */
		var runtimeAppDetailsListBox = $('#runtime-app-details-list-box'),
			runtimeAppConfigListBox = $('#runtime-app-config-list-box');
		runtimeAppDetailsListBox.on('click', 'tr', function(e) {
			if (e.target.nodeName == 'A') {
				return true;
			}
			
			runtimeAppDetailsListBox.find('tr.active').removeClass('active');
			
			var item = $(e.currentTarget);
			item.addClass('active');
			
			var currServer = item.attr('data-id');
			SESSION[SESSION.appName].runtime.currServer = currServer;
			
			var appVersion = item.attr('data-app-version');
			Timer.start('loadTabPanelTask', 'loadRuntimeAppConfigListTask', function() {
				loadRuntimeAppConfigList(SESSION.appName, appVersion);
			}, SESSION.interval.loadRuntimeAppConfigListTask, true);
			
			return false;
			
		});
		function loadTabPanel_runtime(appName) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPSERVERLIST}" />',
				data: {cmServer: SESSION.cmServer, appName: appName},
				success: function(response) {
					var data = response.result;
					$.each(data, function(index, item) {
						item.index = (index + 1);
					});
					
					var currServer = SESSION[appName].runtime.currServer;
					if (currServer) {
						var notFind = true;
						$.each(data, function(index, item) {
							if (item.server == currServer) {
								item.cssClass = 'active';
								notFind = false;
								return false;
							}
						});
						if (notFind) {
							SESSION[appName].runtime.currServer = '';
							runtimeAppConfigListBox.empty();
						}
					} else {
						runtimeAppConfigListBox.empty();
					}
					
					runtimeAppDetailsListBox.empty();
					$('#runtime-app-details-list-item-tpl').tmpl(data).appendTo(runtimeAppDetailsListBox);
				}
			});
		}
		function loadRuntimeAppConfigList(appName, appVersion) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPCONFIG}" />',
				data: {cmServer: SESSION.cmServer, appName: SESSION.appName, appVersion: appVersion},
				success: function(response) {
					var data = response.result;
					runtimeAppConfigListBox.empty();
					$('#runtime-app-config-list-item-tpl').tmpl(data).appendTo(runtimeAppConfigListBox);
				}
			});
		}
		
		/* ---------- CONFIG ------------ */
		var appVersionListBox = $('#app-version-list-box'),
			appConfigListBox = $('#app-config-list-box');;
		appVersionListBox.on('click', 'a', function(e) {
			appVersionListBox.find('.active').removeClass('active');
			
			var item = $(e.currentTarget);
			item.addClass('active');
			
			var currVersion = item.attr('data-id');
			SESSION[SESSION.appName].config.currVersion = currVersion;
			
			Timer.start('loadTabPanelTask', 'loadAppConfigListTask', function() {
				loadAppConfigList(SESSION.appName, currVersion);
			}, SESSION.interval.loadAppConfigListTask, true);
			
			return false;
		});
		function loadTabPanel_config(appName) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPVERSIONLIST}" />',
				data: {cmServer: SESSION.cmServer, appName: appName},
				success: function(response) {
					var data = response.result;
					var currVersion = SESSION[appName].config.currVersion;
					if (currVersion) {
						var notFind = true;
						$.each(data, function(index, item) {
							if (item.version == currVersion) {
								item.cssClass = 'active';
								notFind = false;
								return false;
							}
						});
						if (notFind) {
							appConfigListBox.empty();
							SESSION[appName].config.currVersion = '';
						}
					} else {
						appConfigListBox.empty();
					}
					
					appVersionListBox.empty();
					$('#app-version-list-item-tpl').tmpl(data).appendTo(appVersionListBox);
				}
			});
		}
		function loadAppConfigList(appName, currVersion) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPCONFIG}" />',
				data: {cmServer: SESSION.cmServer, appName: SESSION.appName, appVersion: currVersion},
				success: function(response) {
					var data = response.result;
					appConfigListBox.empty();
					$('#app-config-list-item-tpl').tmpl(data).appendTo(appConfigListBox);
				}
			});
		}
		
		/* ---------- CLI ------------ */
		var cliAppListBox = $('#cli-app-list-box');
		cliAppListBox.on('click', '.server-selector', function(e, forceChecked) {
			if (forceChecked) { // TODO
				$(e.currentTarget).removeAttr('checked');
			}
			
			var selectedServer = $(e.currentTarget).val(),
				currServers = SESSION[SESSION.appName].cli.currServers;
			if ($(e.currentTarget).is(':checked')) {
				if (!currServers[selectedServer]) {
					currServers[selectedServer] = true;
				}
			} else {
				delete currServers[selectedServer];
			}
		});
		$('#cli-app-list-selector').on('click', function(e) {
			cliAppListBox.find('.server-selector').trigger('click'); // trigger('click', [$(e.currentTarget).is(':checked')]);
		});
		cliAppListBox.on('click', '.expand-btn', function(e) {
			var expandBtn = $(e.currentTarget),
				isExpand = expandBtn.hasClass('active'),
				currServer = expandBtn.attr('data-id'),
				expandRows = SESSION[SESSION.appName].cli.expandRows,
				expandRowEl = cliAppListBox.find('.expand-row[data-id="' + currServer + '"]');
			
			if (isExpand) {
				expandBtn.removeClass('active');
				expandRowEl.hide();
				
				delete expandRows[currServer];
			} else {
				expandBtn.addClass('active');
				expandRowEl.show();
				
				if (!expandRows[currServer]) {
					expandRows[currServer] = true;
				}
			}
		});
		
		function loadTabPanel_cli(appName) {
			ajax({
				url: '<e:url value="${P.CONSOLE_CONFIG_APPSERVERCLILIST}" />',
				data: {cmServer: SESSION.cmServer, appName: appName},
				success: function(response) {
					var data = response.result;
					$.each(data, function(index, item) {
						item.index = (index + 1);
						item.cliExecStatusCssClass = {InProgress: 'progress-bar-striped active', Success: 'progress-bar-success', Failure: 'progress-bar-danger', 'Idle': ''}[item.cliExecStatus];
					});
					
					var currServers = SESSION[appName].cli.currServers;
					if (currServers) {
						var notFind = true;
						$.each(data, function(index, item) {
							if (item.server in currServers) {
								item.selected = 'checked';
								notFind = false;
							}
						});
						if (notFind) {
							SESSION[appName].cli.currServers = {};
						}
					}
					var expandRows = SESSION[appName].cli.expandRows;
					if (expandRows) {
						$.each(data, function(index, item) {
							if (item.server in expandRows) {
								item.expandBtnCssClass = 'active';
								item.expandRowCssStyle = 'display:';
							} else {
								item.expandBtnCssClass = '';
								item.expandRowCssStyle = 'display:none';
							}
						});
					}
					
					cliAppListBox.empty();
					$('#cli-app-list-item-tpl').tmpl(data).appendTo(cliAppListBox);
				}
			});
		}
		
		var cliInput = $('#cli-input'),
			cliBtn = $('#cli-btn');
		cliInput.on('keyup', function(e) {
			if (e.which == 13 && cliInput.val()) {
				cliBtn.trigger('click');
			}
		});
		cliBtn.on('click', function(e) {
			var cmd = cliInput.val();
			if (cmd) {
				var currServers = SESSION[SESSION.appName].cli.currServers;
				if (currServers) {
					var servers = [];
					for (var currServer in currServers) {
						servers.push(currServer);
					}
					if (servers.length == 0) {
						alert('请选中应用')
						return false;
					}
					
					cliBtn.attr('disabled',"true");
					ajax({
						url: '<e:url value="${P.CONSOLE_CONFIG_EXECCMD}" />',
						type: 'POST',
						data: {cmServer: SESSION.cmServer, appName: SESSION.appName, servers: servers.join(','), cmd: cmd},
						complete: function() {
							cliBtn.removeAttr("disabled");
						}
					});
				}
			}
		});
		
		
		/* Timer.start('showTask', function() {
			console.dir(Timer)
		}, 1000, true); */
		
		
		/* DIALOG */
		$('#add-app-btn').on('click', function(e) {
			var appNameEl = $('#appName');
			appNameEl.val('');
			
			$('#add-app-form').dialog({
				title: '添加应用',
				buttons: [{
					text: '保存',
					'class': 'btn-primary',
					hotKey: 13,
					click: function() {
						var _t = this, 
							appNameVal = appNameEl.val();
						if (!appNameVal) {
							appNameEl.focus();
							return false;
						}
						
						ajax({
							url: '<e:url value="${P.CONSOLE_CONFIG_ADDAPP}" />',
							type: 'POST',
							data: {cmServer: SESSION.cmServer, appName: appNameVal},
							complete: function() {
								$(_t).dialog('close');
								setTimeout(loadAppList, 1300);
							}
						});
					}
				}, {
					text: '取消',
					'class': 'btn-default',
					click: function() {
						$(this).dialog('close');
					}
				}]
			});
		});
		
		$('#add-version-btn').on('click', function(e) {
			if (!SESSION.appName) {
				alert('请选择应用');
				return false;
			}
			
			var appVersionEl = $('#appVersion');
			appVersionEl.val('');
			
			$('#add-version-form').dialog({
				title: '添加版本',
				buttons: [{
					text: '保存',
					'class': 'btn-primary',
					hotKey: 13,
					click: function() {
						var _t = this, 
							appVersionVal = appVersionEl.val();
						if (!appVersionVal) {
							appVersionEl.focus();
							return false;
						}
						
						ajax({
							url: '<e:url value="${P.CONSOLE_CONFIG_ADDAPPVERSION}" />',
							type: 'POST',
							data: {cmServer: SESSION.cmServer, appName: SESSION.appName, appVersion: appVersionVal},
							complete: function() {
								$(_t).dialog('close');
								setTimeout(function() {
									loadTabPanel_config(SESSION.appName);
								}, 1300);
							}
						});
					}
				}, {
					text: '取消',
					'class': 'btn-default',
					click: function() {
						$(this).dialog('close');
					}
				}]
			});
		});
		
		function showConfigForm(initData){
			if (!SESSION.appName) {
				alert('请选择应用');
				return false;
			}
			
			var appVersion = SESSION[SESSION.appName].config.currVersion;
			if (!appVersion) {
				alert('请选择配置版本');
				return false;
			}
			
			var configAttrKeyEl = $('#configAttrKey'),
				configAttrValueEl = $('#configAttrValue');
			configAttrKeyEl.val(initData.key || '');
			configAttrValueEl.val(initData.value || '');
			
			$('#add-config-form').dialog({
				title: '保存配置属性',
				buttons: [{
					text: '保存',
					'class': 'btn-primary',
					hotKey: 13,
					click: function() {
						var _t = this, 
							configAttrKeyVal = configAttrKeyEl.val();
						if (!configAttrKeyVal) {
							configAttrKeyEl.focus();
							return false;
						}
						var configAttrValueVal = configAttrValueEl.val();
						//if (!configAttrValueVal) {
						//	configAttrValueEl.focus();
						//	return false;
						//}
						
						ajax({
							url: '<e:url value="${P.CONSOLE_CONFIG_SETAPPCONFIG}" />',
							type: 'POST',
							data: {cmServer: SESSION.cmServer, appName: SESSION.appName, appVersion: appVersion, key: configAttrKeyVal, value: configAttrValueVal},
							complete: function() {
								$(_t).dialog('close');
								setTimeout(function() {
									loadAppConfigList(SESSION.appName, appVersion);
								}, 1300);
							}
						});
					}
				}, {
					text: '取消',
					'class': 'btn-default',
					click: function() {
						$(this).dialog('close');
					}
				}]
			});
		}
		
		$('#add-config-btn').on('click', function(e) {
			showConfigForm({key: '', value: ''});
		});
		
		$('#app-config-list-box').on('click', '.modify-config-btn', function(e) {
			var tds = $(e.currentTarget).parents('tr').children('td'),
				key = tds.eq(0).text(),
				value = tds.eq(1).text();
			
			/* try {
				value = JSON.stringify(eval('(' + value + ')'), null, '\t');
			} catch (e) {} */
			
			showConfigForm({key: key, value: value});
		});
		$('#app-config-list-box').on('click', '.delete-config-btn', function(e) {
			var tds = $(e.currentTarget).parents('tr').children('td'),
				key = tds.eq(0).text(),
				value = tds.eq(1).text();
			
			var appVersion = SESSION[SESSION.appName].config.currVersion;
			$.messager.confirm('确认', '是否删除 ' + key + ' ?', function() {
				ajax({
					url: '<e:url value="${P.CONSOLE_CONFIG_DELETEAPPCONFIG}" />',
					type: 'POST',
					data: {cmServer: SESSION.cmServer, appName: SESSION.appName, appVersion: appVersion, key: key},
					complete: function() {
						setTimeout(function() {
							loadAppConfigList(SESSION.appName, appVersion);
						}, 1300);
					}
				});
			});
		});
		
		$.messager.model = {
			ok:{ text: '确认', classed: 'btn-primary' },
			cancel: { text: "取消", classed: 'btn-default' }
		};
	});
</script>
</body>
</html>
package eap.config.module.console.config.clr;

import java.util.Collections;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import eap.EapContext;
import eap.base.BaseController;
import eap.config.mirror.CMMirror;
import eap.config.mirror.CMMirrorRepository;
import eap.config.module.P;

/**
 * <p> Title: </p>
 * <p> Description: </p>
 * @作者 chiknin@gmail.com
 * @创建时间 
 * @版本 1.00
 * @修改记录
 * <pre>
 * 版本       修改人         修改时间         修改内容描述
 * ----------------------------------------
 * 
 * ----------------------------------------
 * </pre>
 */
@Controller
public class ConfigCLR extends BaseController {
	
	@Autowired
	@Qualifier("cmMirrorRepository")
	private CMMirrorRepository cmMirrorRepository;
	private CMMirror getCmMirror(String cmServer) {
		return cmMirrorRepository.getMirror(cmServer, true);
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_INDEX)
	public String index(Model model) {
		String[] cmServers = null;
		String cmServersProp = EapContext.getEnv().getProperty("cm.servers");
		if (cmServersProp != null && cmServersProp.length() > 0) {
			cmServers = cmServersProp.split(" ");
			model.addAttribute("firstCmServers", cmServers[0]);
		} else {
			cmServers = new String[0];
		}
		model.addAttribute("cmServers", cmServers);
		
		return "configIndex";
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_APPLIST, method=RequestMethod.GET)
	@ResponseBody
	public Object appList(@RequestParam("cmServer") String cmServer) {
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null) {
			return Collections.emptyList();
		}
		
		return cmMirror.getAppList();
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_APPSERVERLIST, method=RequestMethod.GET)
	@ResponseBody
	public Object appServerList(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName) 
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0) {
			return Collections.emptyList();
		}
		
		return cmMirror.getAppServerList(appName);
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_APPCONFIG, method=RequestMethod.GET)
	@ResponseBody
	public Object appConfig(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName, 
		@RequestParam("appVersion") String appVersion)
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null 
			|| appName == null || appName.length() == 0
			|| appVersion == null || appVersion.length() == 0) {
			return Collections.emptyList();
		}
		
		return cmMirror.getAppConfig(appName, appVersion);
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_APPVERSIONLIST, method=RequestMethod.GET)
	@ResponseBody
	public Object appVersionList(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName) 
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0) {
			return Collections.emptyList();
		}
		
		return cmMirror.getAppVersionList(appName);
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_APPSERVERCLILIST, method=RequestMethod.GET)
	@ResponseBody
	public Object appServerCliList(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName) 
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0) {
			return Collections.emptyList();
		}
		
		return cmMirror.getAppServerCliList(appName);
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_EXECCMD, method=RequestMethod.POST)
	@ResponseBody
	public Object execCmd(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName,
		@RequestParam("servers") String[] servers,
		HttpServletRequest request)
	{
		String cmd = request.getParameter("cmd");
		
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0 || servers == null || servers.length == 0) {
			return false;
		}
		
		cmMirror.execCmd(appName, servers, cmd);
		return true;
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_ADDAPP, method=RequestMethod.POST)
	@ResponseBody
	public Object addApp(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName,
		HttpServletRequest request)
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0) {
			return false;
		}
		
		cmMirror.addApp(appName);
		return true;
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_ADDAPPVERSION, method=RequestMethod.POST)
	@ResponseBody
	public Object addAppVersion(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName,
		@RequestParam("appVersion") String appVersion,
		HttpServletRequest request)
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null || appName == null || appName.length() == 0 || appVersion == null || appVersion.length() == 0) {
			return false;
		}
		
		cmMirror.addAppVersion(appName, appVersion);
		return true;
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_SETAPPCONFIG, method=RequestMethod.POST)
	@ResponseBody
	public Object setAppConfig(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName,
		@RequestParam("appVersion") String appVersion,
		@RequestParam("key") String key,
		HttpServletRequest request)
	{
		String value = request.getParameter("value"); // this.getParameterAsCleanHtml("value");
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null 
				|| appName == null || appName.length() == 0 
				|| appVersion == null || appVersion.length() == 0
				|| key == null || key.length() == 0) { // || value == null || value.length() == 0
			return false;
		}
		
		cmMirror.setAppConfig(appName, appVersion, key, value);
		return true;
	}
	
	@RequestMapping(value=P.CONSOLE_CONFIG_DELETEAPPCONFIG, method=RequestMethod.POST)
	@ResponseBody
	public Object deleteAppConfig(
		@RequestParam("cmServer") String cmServer, 
		@RequestParam("appName") String appName,
		@RequestParam("appVersion") String appVersion,
		@RequestParam("key") String key,
		HttpServletRequest request)
	{
		CMMirror cmMirror = getCmMirror(cmServer);
		if (cmMirror == null 
				|| appName == null || appName.length() == 0 
				|| appVersion == null || appVersion.length() == 0
				|| key == null || key.length() == 0) {
			return false;
		}
		
		cmMirror.deleteAppConfig(appName, appVersion, key);
		return true;
	}
}
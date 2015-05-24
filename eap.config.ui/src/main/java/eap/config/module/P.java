package eap.config.module;

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
public interface P {
	
	String ROOT = "/";
	
	/** 错误页.404 */
	String ERROR_PAGE_404 = "/404.htm";
	/** 错误页.404 */
	String ERROR_PAGE_409 = "/409.htm";
	/** 错误页.500 */
	String ERROR_PAGE_500 = "/500.htm";
	
	String CONSOLE_CONFIG_INDEX = "/console/config/index.htm";
	
	String CONSOLE_CONFIG_APPLIST = "/console/config/appList.rest";
	String CONSOLE_CONFIG_APPSERVERLIST = "/console/config/appServerList.rest";
	String CONSOLE_CONFIG_APPCONFIG = "/console/config/appConfig.rest";
	String CONSOLE_CONFIG_APPVERSIONLIST = "/console/config/appVersionList.rest";
	String CONSOLE_CONFIG_APPSERVERCLILIST = "/console/config/appServerCliList.rest";
	String CONSOLE_CONFIG_EXECCMD = "/console/config/execCmd.rest";
	
	String CONSOLE_CONFIG_ADDAPP = "/console/config/addApp.rest";
	String CONSOLE_CONFIG_ADDAPPVERSION = "/console/config/addAppVersion.rest";
	String CONSOLE_CONFIG_SETAPPCONFIG = "/console/config/setAppConfig.rest";
	String CONSOLE_CONFIG_DELETEAPPCONFIG = "/console/config/deleteAppConfig.rest";
}

package eap.config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eap.config.ConfigManager.NodeListener;

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
public class CliNodeListener extends NodeListener {
	private static final Logger logger = LoggerFactory.getLogger(CliNodeListener.class);
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void nodeChanged(CuratorFramework client, ChildData childData) throws Exception {
		byte[] dataBytes = childData != null ? childData.getData() : null;
		if (dataBytes != null && dataBytes.length > 0) {
			String data = new String(dataBytes).trim();
			String[] dataArr = data.split("\r\n");
			if (dataArr.length > 1) {
				return;
			}
			logger.info("received data: " + data);
			
			String cliDataStr = dataArr[0];
			
			try {
				client.setData().forPath(childData.getPath(), (cliDataStr + "\r\n" + "InProgress").getBytes());
				
				if ("now".equalsIgnoreCase(cliDataStr)) {
					cliDataStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
				} else {
					CliData cliData = parseCliData(cliDataStr);
					String className = cliData.getClassName();
					String methodName = cliData.getMethod();
					String[] args = cliData.getArgs();
					
					Class clazz = Class.forName(className);
					Method method = args != null ? clazz.getDeclaredMethod(methodName, String[].class) : clazz.getDeclaredMethod(methodName, null);
					Object obj = null;
					if ((method.getModifiers() & Modifier.STATIC) != 0) {
						obj = clazz;
					} else {
						obj = clazz.newInstance();
					}
					
					if (args != null) {
						method.invoke(obj, (Object)args);
					} else {
						method.invoke(obj);
					}
				}
				
				client.setData().forPath(childData.getPath(), (cliDataStr + "\r\n" + "Success").getBytes());
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				
				String message = null;
				if (e instanceof InvocationTargetException) {
					InvocationTargetException ite = (InvocationTargetException) e;
					message = ite.getMessage();
				} else {
					message = e.getMessage();
				}
				client.setData().forPath(childData.getPath(), (cliDataStr + "\r\n" + "Failure" + "\r\n" + e.getClass() + ": " + message).getBytes());
			}
		}
	}
	
	private CliData parseCliData(String cliDataStr) {
		CliData cliData = new CliData();
		
		try {
			String[] a1 = cliDataStr.split(" ", 3);
			cliData.setClassName(a1[0]);
			cliData.setMethod(a1[1]);
			
			String args = a1[2];
			int argsLen = args.length();
			StringBuilder arg = new StringBuilder();
			boolean skip = false;
			List<String> argList = new ArrayList<String>(); 
			for (int i = 0; i < argsLen; i++) {
				char c = args.charAt(i);
				if (c == '\"') {
					skip = !skip;
					continue;
				}
				if ((!skip && c == ' ')) {
					argList.add(arg.toString());
					arg.setLength(0);
					continue;
				}
				arg.append(c);
			}
			argList.add(arg.toString());
			cliData.setArgs(argList.toArray(new String[]{}));
		} catch (Exception e) {
			throw new IllegalArgumentException("cli error, " + e.getClass() + "-> " + e.getMessage(), e);
		}
		
		return cliData;
	}
	
	private static class CliData {
		
		private String className;
		private String method;
		private String[] args;
		
		public String getClassName() {
			return className;
		}
		public void setClassName(String className) {
			this.className = className;
		}
		public String getMethod() {
			return method;
		}
		public void setMethod(String method) {
			this.method = method;
		}
		public String[] getArgs() {
			return args;
		}
		public void setArgs(String[] args) {
			this.args = args;
		}
		
		@Override
		public String toString() {
			StringBuilder bud = new StringBuilder();
			bud.append(className + "." + method + "(");
			if (args != null && args.length > 0) {
				for (String arg : args) {
					bud.append("\"" + arg + "\",");
				}
				bud.deleteCharAt(bud.length() - 1);
			}
			bud.append(")");
			
			return bud.toString();
		}
	}
	
	public static void main(String[] args) {
		String data = "eap.CliNodeListener test 1 \"2 3\" 4 \"5 6 7 8\"";
		CliData cd = new CliNodeListener().parseCliData(data);
		System.out.println(cd);
	}
}
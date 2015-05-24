package eap.config;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Title: </p>
 * <p> Description: 
 * <pre>
 *   app.name: 应用名称
 *   app.id: 应用ID
 *   app.version: 应用版本
 *   app.server.ip=应用服务器IP
 *   app.server.port=应用服务器端口
 *   app.password=应用密码
 *   app.CMServer=ZooKeeper服务器连接地址
 *   app.CMServer.cluster=DEFAULT
 *   app.CMServer.retryNum=与ZooKeeper重新连接次数
 *   app.CMServer.retryTimes=与ZooKeeper重新连接间隔时间
 *   app.CMServer.connectionTimeoutMs=与ZooKeeper连接超时时间
 *  
 * -Dapp.name="WebDemo" -Dapp.id="1" -Dapp.version="1.0.0" -Dapp.CMServer="node1:2181,node2:2182,node3:2183"
 * SAMPLE(Zookeeper Tree): 
 *  /eapconfig/DEFAULT/WebDemo
 *     /config
 *         /1.0.0
 *             /env
 *             /log
 *        /1.0.1
 *            /env
 *            /log
 *     /runtime
 *         /leader
 *           DATA: 192.168.1.10:8080
 *         /server
 *             /192.168.1.10:8080
 *               health
 *                 DATA: {"app.id": "1", "app.version": "1.0.1", "app.startTime": "2014-05-21 21:10:07", "pid": "123"}
 *               cli
 *             /192.168.1.11:8080
 *               health
 *                 DATA: {"app.id": "2", "app.version": "1.0.0", "app.startTime": "2014-04-23 04:20:13", "pid": "421"}
 *               cli
 *         /locking
 * </pre>
 * </p>
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
public class ConfigManager {
	
	private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);
	
	private static Properties envProps = new Properties();
	static {
		InputStream envInputStream = null;
		try {
			URL envUrl = ConfigManager.class.getResource("/env.properties");
			if (envUrl != null) {
				envInputStream = new FileInputStream(envUrl.getPath());
				envProps.load(envInputStream);
			}
		} catch (Exception e) {
			logger.debug(e.getMessage(), e);
		} finally {
			try {
				if (envInputStream != null) {
					envInputStream.close();
				}
			} catch (Exception e) {}
		}
	}
	
	public static final byte[] EMPTY_DATE = new byte[0];
	
	private static CuratorFramework client;
	private static boolean started = false;
//	private static MultiValueMap<String, NodeListener> nodeListenersMap = CollectionUtils.toMultiValueMap(new ConcurrentHashMap<String, List<NodeListener>>());
	private static Map<String, Map<NodeListener, List<Closeable>>> cache = new ConcurrentHashMap<String, Map<NodeListener, List<Closeable>>>();
	
	public static String cmCluster;
	
	public static String appName;
	public static Long appId;
	public static String appVersion;
	public static String cmConfigNS;
	public static String envPath;
	public static String cmRuntimeNS;
	public static String leaderPath;
	public static String serverPath;
	public static String lockingPath;
//	public static String cliPath;
	
	public static String serverIp;
	public static String serverPort;
	public static String serverId;
	public static String serverIdPath;
	public static String serverHealthPath;
	public static String serverCliPath;
	
	private static LeaderLatch leaderLatch;
	
	public static synchronized void start() throws Exception {
		if (started) {
			return;
		}
		
		String appCMServer = System.getProperty("app.CMServer", envProps.getProperty("app.CMServer")); // TODO , "localhost:2181"
		if (appCMServer != null && appCMServer.length() > 0) {
			Integer retryNum = new Integer(System.getProperty("app.CMServer.retryNum", envProps.getProperty("app.CMServer.retryNum", Integer.MAX_VALUE + "")));
			Integer retryTimes = new Integer(System.getProperty("app.CMServer.retryTimes", envProps.getProperty("app.CMServer.retryTimes", 3000 + "" )));
			Integer connectionTimeoutMs = new Integer(System.getProperty("app.CMServer.connectionTimeoutMs", envProps.getProperty("app.CMServer.connectionTimeoutMs", 10000 + "")));
			
			final CountDownLatch connectedLatch = new CountDownLatch(1);
			client = CuratorFrameworkFactory.builder()
				.connectString(appCMServer)
				.retryPolicy(new RetryNTimes(retryNum, retryTimes))
				.connectionTimeoutMs(connectionTimeoutMs).build();
			client.getCuratorListenable().addListener(new CuratorListener() {
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					if (connectedLatch.getCount() > 0 && event.getWatchedEvent() != null && event.getWatchedEvent().getState() == KeeperState.SyncConnected) {
						connectedLatch.countDown();
					}
				}
			});
			client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
				public void unhandledError(String message, Throwable e) {
					e.printStackTrace();
					logger.error(message, e);
				}
			});
			client.start();
			connectedLatch.await();
			
			init();
			
			started = true;
			
			addListener(serverCliPath, new CliNodeListener()); // TODO
		}
	}
	
	private static void init() throws Exception {
		cmCluster = envProps.getProperty("app.CMServer.cluster", "DEFAULT");
		appName = envProps.getProperty("app.name", System.getProperty("app.name"));
		if (appName == null || appName.length() == 0) {
			throw new IllegalArgumentException("system property 'app.name' must not be empty");
		}
		String appIdStr = System.getProperty("app.id");
		if (appIdStr == null || appIdStr.length() == 0) {
			throw new IllegalArgumentException("system property 'app.id' must not be empty");
		}
		try {
			appId = new Long(appIdStr);
		} catch (Exception e) {
			throw new IllegalArgumentException("system property 'app.id' must be of type long");
		}
		appVersion = envProps.getProperty("app.version", System.getProperty("app.version", "0"));
		serverIp = System.getProperty("app.server.ip", getLocalAddress());
		serverPort = System.getProperty("app.server.port", getServerHttpPort());
		serverId = serverIp + ":" + serverPort;
		
		String appRootPath = String.format("/eapconfig/%s/%s", cmCluster, appName);
		cmConfigNS = String.format(appRootPath + "/config/%s", appVersion);
		envPath = cmConfigNS + "/env";
		cmRuntimeNS = appRootPath + "/runtime";
		serverPath = cmRuntimeNS + "/server";
		lockingPath = cmRuntimeNS + "/locking";
		leaderPath = cmRuntimeNS + "/leader";
		serverIdPath = serverPath + "/" + serverId;
		
		for (String p : new String[] {cmConfigNS, envPath, cmRuntimeNS, serverPath, lockingPath, serverIdPath}) {
			if (client.checkExists().forPath(p) == null) {
				client.create()
					.creatingParentsIfNeeded()
					.withMode(CreateMode.PERSISTENT)
					.withACL(Ids.OPEN_ACL_UNSAFE)
					.forPath(p, EMPTY_DATE);
			}
		}
		
		serverHealthPath = serverIdPath + "/health";
		if (client.checkExists().forPath(serverHealthPath) != null) {
			client.delete().forPath(serverHealthPath);
		}
		client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.withACL(Ids.OPEN_ACL_UNSAFE)
			.forPath(serverHealthPath, String.format("{\"app.id\":\"%d\",\"app.version\":\"%s\",\"app.startTime\":\"%s\",\"pid\":\"%s\"}", appId, appVersion, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), getPid()).getBytes());
		
		serverCliPath = serverIdPath + "/cli";
		if (client.checkExists().forPath(serverCliPath) != null) {
			client.delete().forPath(serverCliPath);
		}
		client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.withACL(Ids.OPEN_ACL_UNSAFE)
			.forPath(serverCliPath, "".getBytes());
		
		leaderLatch = new LeaderLatch(client, leaderPath, serverId);
		leaderLatch.start();
	}
	
	public static void addListener(final String path, final NodeListener listener) throws Exception {
		if (isStarted()) {
			if (cache.get(path) != null && cache.get(path).get(listener) != null) { // listener exists 
				return;
			}
			
			final NodeCache nodeCache = new NodeCache(client, path);
			nodeCache.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					listener.nodeChanged(client, nodeCache.getCurrentData());
				}
			});
			nodeCache.start();
			
			final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, false);
			pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					listener.childEvent(client, event);
				}
			});
			pathChildrenCache.start();
			
			putToCache(path, listener, nodeCache, pathChildrenCache);
		}
	}
	private static void putToCache(String path, NodeListener listener, Closeable... closeables) {
		Map<NodeListener, List<Closeable>> m = cache.get(path);
		if (m == null) {
			m = new ConcurrentHashMap<NodeListener, List<Closeable>>();
			cache.put(path, m);
		}
		m.put(listener, Arrays.asList(closeables));
	}
	
	public static void removeListener(String path, NodeListener listener) throws Exception {
		Map<NodeListener, List<Closeable>> m = cache.get(path);
		if (m != null && m.size() > 0 && m.containsKey(listener)) {
			for (Closeable closeable : m.get(listener)) {
				try {
					closeable.close();
				} catch (IOException e) {}
			}
		}
	}
	public static void removeListener(String path) throws Exception {
		Map<NodeListener, List<Closeable>> m = cache.get(path);
		if (m != null && m.size() > 0) {
			for (NodeListener listener : m.keySet()) {
				removeListener(path, listener);
			}
		}
	}
	public static void removeAllListener() throws Exception {
		for (String path : cache.keySet()) {
			removeListener(path);
		}
	}
	
	public static boolean isLeader() {
		if (isStarted() && leaderLatch != null) {
			try {
				return serverId.equals(leaderLatch.getLeader().getId());
			} catch (Exception e) {
				logger.debug(e.getMessage(), e);
				return false;
			}
		} else {
			return true;
		}
	}
	
	public static synchronized void stop() {
		if (isStarted() && client != null && client.getState() == CuratorFrameworkState.STARTED) {
			if (leaderLatch != null) {
				try {
					leaderLatch.close();
				} catch (IOException e) {}
			}
			
			client.close();
			
			started = false;
		}
	}
	
	
	public static boolean isStarted() {
		return started;
	}
	
	public static boolean isEnabled() {
		String appCMServer = System.getProperty("app.CMServer", envProps.getProperty("app.CMServer")); // TODO , "localhost:2181"
		return (appCMServer != null && appCMServer.length() > 0) ? true : false;
	}
	
	public static class NodeListener {
		public void nodeChanged(CuratorFramework client, ChildData childData) throws Exception {
		}
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		}
	}
	
	public static void setNodeData(String path, byte[] data) {
		if (isStarted()) {
			try {
				client.setData().forPath(path, data);
			} catch (Exception e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
	}
	public static String getNodeData(String path) {
		if (isStarted()) {
			try {
				byte[] data = client.getData().forPath(path);
				return data != null ? new String(data) : null;
			} catch (Exception e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
		return null;
	}
	public static List<String> getNodeList(String path) {
		if (isStarted()) {
			try {
				return client.getChildren().forPath(path);
			} catch (Exception e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
		return Collections.EMPTY_LIST;
	}
	public static CuratorFramework getClient() {
		return client;
	}
	
	// ----------------------
	// tools
	// ----------------------
	private static String getLocalAddress() {
		try {
			// 遍历网卡，查找一个非回路ip地址并返回
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			ArrayList<String> ipv4Result = new ArrayList<String>();
			ArrayList<String> ipv6Result = new ArrayList<String>();
			while (enumeration.hasMoreElements()) {
				final NetworkInterface networkInterface = enumeration.nextElement();
				final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
				while (en.hasMoreElements()) {
					final InetAddress address = en.nextElement();
					if (!address.isLoopbackAddress()) {
						if (address instanceof Inet6Address) {
							ipv6Result.add(normalizeHostAddress(address));
						}
						else {
							ipv4Result.add(normalizeHostAddress(address));
						}
					}
				}
			}

			// 优先使用ipv4
			if (!ipv4Result.isEmpty()) {
				for (String ip : ipv4Result) {
					if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
						continue;
					}

					return ip;
				}
				
				// 取最后一个
				return ipv4Result.get(ipv4Result.size() - 1);
			}
			// 然后使用ipv6
			else if (!ipv6Result.isEmpty()) {
				return ipv6Result.get(0);
			}
			// 然后使用本地ip
			final InetAddress localHost = InetAddress.getLocalHost();
			return normalizeHostAddress(localHost);
		}
		catch (SocketException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
		catch (UnknownHostException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	private static String normalizeHostAddress(final InetAddress localHost) {
		if (localHost instanceof Inet6Address) {
			return "[" + localHost.getHostAddress() + "]";
		}
		else {
			return localHost.getHostAddress();
		}
	}
	private static String getPid() {
		String name = ManagementFactory.getRuntimeMXBean().getName();
		if (name != null && name.length() > 0 && name.indexOf("@") != -1) {
			return name.split("@")[0];
		}
		
		return null;
	}
	private static String getServerHttpPort() {
		return getServerPort("HTTP/1.1", "http");
	}
	private static String getServerPort(String protocol, String scheme) { 
		MBeanServer mBeanServer = null; 
		if (MBeanServerFactory.findMBeanServer(null).size() > 0) { 
			mBeanServer = (MBeanServer) MBeanServerFactory.findMBeanServer(null).get(0);
		} 
		if (mBeanServer == null) {
			return null;
		}
		
		Set names = null; 
		try { 
			names = mBeanServer.queryNames(new ObjectName("Catalina:type=Connector,*"), null); // TODO support tomcat
		} catch (Exception e) { 
			return null;
		}
		
		try {
			Iterator it = names.iterator(); 
			ObjectName oname = null; 
			while (it.hasNext()) { 
				oname = (ObjectName)it.next(); 
				String pvalue = (String) mBeanServer.getAttribute(oname, "protocol"); 
				String svalue = (String)mBeanServer.getAttribute(oname, "scheme"); 
				if (protocol.equalsIgnoreCase(pvalue) && scheme.equalsIgnoreCase(svalue)) { 
					return ((Integer) mBeanServer.getAttribute(oname, "port")).toString(); 
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
		return null;
	}
	
	public static void main(String[] args) throws Exception {
		
		ConfigManager.start();
		
		if (ConfigManager.isStarted()) {
			System.out.println("Started");
		}
//		for (int i = 1; i < 100; i++) {
			System.out.println(ConfigManager.isLeader());
			
			NodeListener l1 = new NodeListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
//					System.out.println("childEvent : " + new String(event.getData().getData()));
				}
				@Override
				public void nodeChanged(CuratorFramework client, ChildData childData) throws Exception {
					System.out.println("nodeChanged : " + new String(childData.getData()));
				}
			};
			ConfigManager.addListener(ConfigManager.envPath, l1);
//			UM.addListener(UM.envPath, l1);
//			UM.addListener(UM.cliPath, l1);
			
//			System.out.println("is loader: " + UM.isLeader());
			
//			UM.removeListener(UM.envPath, l1);
//			
//			Thread.sleep(5000);
//			
//			UM.addListener(UM.envPath, l1);
			
//			UM.removeListener(UM.envPath);
			
			Thread.sleep(10000000);
//		}
		
		
		ConfigManager.stop();
	}
}
package eap.config.mirror;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class NodeWatcher {
	
	private static final Logger logger = LoggerFactory.getLogger(NodeWatcher.class);
	
	private Map<String, List<Closeable>> cache = new ConcurrentHashMap<String, List<Closeable>>();
	
	private CuratorFramework client;
	private NodeListener listener;
	
	public NodeWatcher(CuratorFramework client, NodeListener listener) {
		this.client = client;
		this.listener = listener;
	}
	
	public void watch(final String path) throws Exception {
		if (cache.get(path) != null) { // listener exists 
			return;
		}
		
		final NodeWatcher _this = this;
		
		final NodeCache nodeCache = new NodeCache(client, path);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				listener.nodeChanged(client, path, nodeCache.getCurrentData(), _this);
			}
		});
		nodeCache.start();
		
		final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, false);
		pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				listener.childEvent(client, event, _this);
			}
		});
		pathChildrenCache.start();
		
		putToCache(path, nodeCache, pathChildrenCache);
		
		List<String> children = client.getChildren().forPath(path);
		if (children != null && children.size() > 0) {
			for (String child : children) {
				watch(path + "/" + child);
			}
		}
	}
	private void putToCache(String path, Closeable... closeables) {
		List<Closeable> list = cache.get(path);
		if (list == null) {
			list = Arrays.asList(closeables);
		}
		cache.put(path, list);
	}
	
	public void unwatch(final String path) {
		if (cache.get(path) != null) { // listener exists 
			return;
		}
		
		List<Closeable> listeners = cache.get(path);
		if (listeners != null && listeners.size() > 0) {
			cache.remove(path);
			for (Closeable listener : listeners) {
				try {
					listener.close();
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public static class NodeListener {
		public void nodeChanged(CuratorFramework client, String path, ChildData childData, NodeWatcher watcher) throws Exception {
		}
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event, NodeWatcher watcher) throws Exception {
		}
	}
}

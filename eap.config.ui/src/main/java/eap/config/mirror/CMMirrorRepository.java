package eap.config.mirror;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

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
@Service("cmMirrorRepository")
public class CMMirrorRepository {
	
	private Map<String, CMMirror> cmMirrors = new ConcurrentHashMap<String, CMMirror>();
//	private ReentrantLock lock = new ReentrantLock();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	
	public CMMirror getMirror(String cmServer, boolean createForNotFound) {
		if (cmServer == null || cmServer.length() == 0) {
			return null;
		}
		
		CMMirror cmMirror = null;
		try {
			
			lock.readLock().lock();
			cmMirror = cmMirrors.get(cmServer);
		} finally {
			lock.readLock().unlock();
		}
		if (cmMirror != null) {
			return cmMirror;
		}
		
		try {
			lock.writeLock().lock();
			if (createForNotFound) {
				try {
					cmMirror = new CMMirror(cmServer);
					cmMirrors.put(cmServer, cmMirror);
				} catch (Exception e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}
		} finally {
			lock.writeLock().unlock();
		}
		
		return cmMirror;
	}
	
	public void shutdownMirror(String cmServer) {
		CMMirror cmMirror = cmMirrors.get(cmServer);
		if (cmMirror != null) {
			cmMirror.shutdown();
		}
	}
	
	@PreDestroy
	public void shutdown() {
		for (CMMirror cmMirror : cmMirrors.values()) {
			cmMirror.shutdown();
		}
	}
}

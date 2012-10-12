package mxp.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.Lock;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisLock extends Lock {
	
	String name;
	ShardedJedisPool pool;
	
	public RedisLock(String nm, ShardedJedisPool pl) {
		name = nm;
		pool = pl;
	}

	@Override
	public boolean isLocked() throws IOException {
		ShardedJedis jds = pool.getResource();
		boolean ret = jds.exists(name.concat(".lock"));
		pool.returnResource(jds);
		return ret;
	}

	@Override
	public boolean obtain() throws IOException {
		if( isLocked() )
			return false;
		ShardedJedis jds = pool.getResource();
		String ret = jds.set(name+".lock", "1");
		pool.returnResource(jds);
		return ret != null;
	}

	@Override
	public void release() throws IOException {
		ShardedJedis jds = pool.getResource();
		jds.del(name+".lock");
		pool.returnResource(jds);
	}

}

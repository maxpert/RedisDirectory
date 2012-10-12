package mxp.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisLockFactory extends LockFactory {
	
	protected ShardedJedisPool pool;
	
	public RedisLockFactory(ShardedJedisPool pl) {
		pool = pl;
	}

	@Override
	public void clearLock(String name) throws IOException {
		ShardedJedis jds = pool.getResource();
		jds.del(name+".lock");
		pool.returnResource(jds);
	}

	@Override
	public Lock makeLock(String name) {
		return new RedisLock(name, pool);
	}

}

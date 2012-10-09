package mxp.solr.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mxp.lucene.store.RedisDirectory;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

public class RedisDirectoryFactory extends DirectoryFactory{
	
	protected ShardedJedisPool pool;
	
	@Override
	public void init(NamedList list) {
		super.init(list);
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		JedisShardInfo si = new JedisShardInfo("localhost", 6379);
		shards.add(si);
		pool = new ShardedJedisPool(new GenericObjectPool.Config(), shards);
	}

	@Override
	public Directory open(String name) throws IOException {
		RedisDirectory redisDir = new RedisDirectory(name, pool);
		if( !redisDir.exists() ) {
			IndexWriter writer = new IndexWriter(redisDir, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)));
			writer.commit();
			writer.close();
		}
		return redisDir;
	}

}

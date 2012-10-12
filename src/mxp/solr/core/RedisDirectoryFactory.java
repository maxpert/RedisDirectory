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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

public class RedisDirectoryFactory extends DirectoryFactory{
	private transient static Logger log = LoggerFactory.getLogger(RedisDirectoryFactory.class);
	protected ShardedJedisPool pool;
	
	private List<JedisShardInfo> loadShardsInfo(SolrParams params) {
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		log.info(">>>>>>>>>>>>>>>>>>>>> Redis directory storage initailizing...");
		for(String conStr: params.getParams("shard")){ 
			try{
				String[] info = conStr.split(":");
				String host = info[0];
				int port = Integer.parseInt(info[1]);
				log.info(String.format(" + Adding shard %s:%d", host, port));
				JedisShardInfo si = new JedisShardInfo(host, port);
				shards.add(si);
			}catch(Exception e){
				log.error(String.format("Invalid shard info %s (connection string should be [host]:[port])", conStr ));
			}
		}
		log.info(">>> Redis shards information parsed...");
		return shards;
	}
	
	@Override
	public void init(@SuppressWarnings("rawtypes") NamedList list) {
		super.init(list);
		SolrParams params = SolrParams.toSolrParams( list );
		List<JedisShardInfo> info = loadShardsInfo(params);
		RedisDirectory.FILE_BUFFER_SIZE = params.getInt("fileBufferSize", RedisDirectory.FILE_BUFFER_SIZE);
		log.info(String.format("File buffer size for Redis shards %d...", RedisDirectory.FILE_BUFFER_SIZE));
		pool = new ShardedJedisPool(new GenericObjectPool.Config(), info);
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

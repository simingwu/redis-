
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.stereotype.Service;



@Service
public class RedisService {
	/** 时效时间 30分钟*/
	private static final Long FAILURE_TIME=86400L;
	@Autowired
    private StringRedisTemplate redisTemplate;

    private static final Logger log = LoggerFactory.getLogger(RedisService.class);

    //用于比较并删除用的LUA脚本
    public static final String COMPARE_AND_DEL =
            "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
            "    return redis.call(\"del\",KEYS[1])\n" +
            "else\n" +
            "    return 0\n" +
            "end";

    //用于比较并更新的LUA脚本
    public static final String COMPARE_AND_UPDATE=
            "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
                    "    return redis.call(\"set\",KEYS[1],ARGV[2])\n" +
                    "else\n" +
                    "    return 0\n" +
                    "end";

    /**
     * 写入缓存
     * @param key
     * @param value
     * @return
     */
    public boolean set(final String key, String value) {
        boolean result = false;
        try {
            ValueOperations<String, String> operations = redisTemplate.opsForValue();
            operations.set(key, value);
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    /**
     * 写入缓存设置时效时间
     * @param key
     * @param value
     * @return
     */
    public boolean set(final String key, String value, Long expireTime) {
        boolean result = false;
        try {
            ValueOperations<String, String> operations = redisTemplate.opsForValue();
            operations.set(key, value);
            redisTemplate.expire(key, expireTime, TimeUnit.SECONDS);
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 写入缓存设置时效时间
     * @param key
     * @param value
     * @param expireTime TimeUnit.SECONDS
     * @return
     * @auther zhouyan
     */
    public boolean setWithAtomic(final String key, String value, int expireTime) {
        boolean result = false;
        try {
            ValueOperations<String, String> operations = redisTemplate.opsForValue();
            operations.set(key, value, expireTime, TimeUnit.SECONDS);
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 批量删除对应的value
     * @param keys
     */
    public void remove(final String... keys) {
        for (String key : keys) {
            remove(key);
        }
    }
    /**
     * 批量删除key
     * @param pattern
     */
    public void removePattern(final String pattern) {
        Set<String> keys = redisTemplate.keys(pattern);
        if (keys.size() > 0) {
            redisTemplate.delete(keys);
        }
    }
    /**
     * 删除对应的value
     * @param key
     */
    public void remove(final String key) {
        if (exists(key)) {
            redisTemplate.delete(key);
        }
    }
    /**
     * 判断缓存中是否有对应的value
     * @param key
     * @return
     */
    public boolean exists(final String key) {
        return redisTemplate.hasKey(key);
    }
    /**
     * 读取缓存
     * @param key
     * @return
     */
    public Object get(final String key) {
        Object result = null;
        ValueOperations<String, String> operations = redisTemplate.opsForValue();
        result = operations.get(key);
        return result;
    }
    /**
     * 哈希 添加
     * @param key
     * @param hashKey
     * @param value
     */
    public void hmSet(String key, Object hashKey, Object value){
        HashOperations<String, Object, Object>  hash = redisTemplate.opsForHash();
        hash.put(key,hashKey,value);
    }

    /**
     * 哈希获取数据
     * @param key
     * @param hashKey
     * @return
     */
    public Object hmGet(String key, Object hashKey){
        HashOperations<String, Object, Object>  hash = redisTemplate.opsForHash();
        return hash.get(key,hashKey);
    }

    /**
     * 列表添加
     * @param k
     * @param v
     */
    public void lPush(String k,String v){
        ListOperations<String, String> list = redisTemplate.opsForList();
        list.rightPush(k,v);
    }

    /**
     * 列表获取
     * @param k
     * @param l
     * @param l1
     * @return
     */
    public List<String> lRange(String k, long l, long l1){
        ListOperations<String, String> list = redisTemplate.opsForList();
        return list.range(k,l,l1);
    }

    /**
     * 集合添加
     * @param key
     * @param value
     */
    public void add(String key,String[] value){
        SetOperations<String, String> set = redisTemplate.opsForSet();
        set.add(key,value);
    }

    /**
     * 集合获取
     * @param key
     * @return
     */
    public Set<String> setMembers(String key){
        SetOperations<String, String> set = redisTemplate.opsForSet();
        return set.members(key);
    }

    /**
     * 集合元素获取,smembers在元素过多时会产生
     * @param key
     * @return
     * @auther zhouyan
     */
    public Cursor<String> setScan(String key){
        SetOperations<String, String> set = redisTemplate.opsForSet();
        //warning: what's the use of the OptionScan with a redisCallback?
        return set.scan(key, ScanOptions.NONE);
        //return set.scan(key, ScanOptions.scanOptions().build());
    }


    /**
     * 有序集合添加
     * @param key
     * @param value
     * @param scoure
     */
    public void zAdd(String key,String value,double scoure){
        ZSetOperations<String, String> zset = redisTemplate.opsForZSet();
        zset.add(key,value,scoure);
    }

    /**
     * 有序集合获取
     * @param key
     * @param scoure
     * @param scoure1
     * @return
     */
    public Set<String> rangeByScore(String key,double scoure,double scoure1){
        ZSetOperations<String, String> zset = redisTemplate.opsForZSet();
        return zset.rangeByScore(key, scoure, scoure1);
    }
    /**
	 * 根据key和value增加有时效的保存
	 * @param key
	 * @param value
	 */
	public void delSetex(String key,String value) {
		try {
            remove(key);
            set(key,value,FAILURE_TIME);

		} catch (Exception e) {
			System.out.println("redis异常----------------------根据key和value增加有时效的保存");
		}
	}
	
	/**
	 * 根据key和value判断更新，如果更新成功返回true
	 * @param key
	 * @param value
	 */
	public boolean updateSetex(String key,String value) {
		try {
            String valueGet = (String) get(key);
            if(StringUtils.isNotBlank(valueGet)){
            	if(value.equals(valueGet)){

//                	RedisUtil.del(key);
                	set(key,value,FAILURE_TIME);

                	return true;
                }else{
                	return false;
                }
            }else{
            	return false;
            }
            
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 根据key清除缓存(校验value)
	 * @param key
	 */
	public boolean delValue(String key,String value) {
		try {
			String valueGet = (String) get(key);
            if(StringUtils.isNotBlank(valueGet)){
	            if(value.equals(valueGet)){
	            	remove(key);
	            	return true;
	            }else{
	            	return false;
	            }
            }else{
            	return true;//已经登出
            }
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}


    /**
     * 执行lua脚本
     * 利用redis自带的lua解释器完成原子性redis调用
     * @param scriptPath luaScript脚本路径
     * @param keyList List设置lua的KEYS
     * @param argvString
     * @auther zhouyan
     */
    public <T> T execLuaScript(Class<T> targetClass, String scriptPath, List<String> keyList, String... argvString){
        DefaultRedisScript<T> getRedisScript = new DefaultRedisScript();
        getRedisScript.setResultType(targetClass);
        //warning:jar包内可能未必好使,需要使用InputStream来代替resource,待测试
        getRedisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(scriptPath)));
        log.debug("script content:"+getRedisScript.getScriptAsString());
        return redisTemplate.execute(getRedisScript, keyList, argvString);
    }

    /**
     * 执行lua脚本,手动传输literal
     * @param scriptContent lua脚本文本
     * @param keyList List设置lua的KEYS
     * @param argvString
     * @auther zhouyan
     */
    public <T> T execLuaLiteral(Class<T> targetClass, String scriptContent, List<String> keyList, String... argvString){
        DefaultRedisScript<T> redisScript = new DefaultRedisScript();
        redisScript.setResultType(targetClass);
        //从DefaultScriptExcutor的scriptBytes方法和RedisScript接口要求的getScriptAsString来看,可行
        redisScript.setScriptSource(new StaticScriptSource(scriptContent)); // =>init constructor setScript()=>redisScript.setScriptText(scriptText)
        log.debug("script content:"+redisScript.getScriptAsString());
        return redisTemplate.execute(redisScript, keyList, argvString);
    }

}
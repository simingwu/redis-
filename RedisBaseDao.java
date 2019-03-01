package com.lianggzone.utils.framework.spring.dao.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Repository;

/**
 * <h3>概要:</h3><p>RedisBaseDao</p>
 * <h3>功能:</h3><p>REDIS工具类接口</p>
 * <h3>履历:</h3>
 * <li>2016年01月13日  v0.1 版本内容: 新建</li>
 * <li>2016年03月10日  v0.1 版本内容: 修改</li>
 * @author 粱桂钊
 * @since 0.1
 */
@Repository
public class RedisBaseDao {

    @Resource(name = "redisTemplate")
    protected RedisTemplate<String, String> redisTemplate;  
    
    @Resource(name="redisTemplate")
    protected HashOperations<String, String, String> hashOperations;
    
    @Resource(name="redisTemplate")
    protected ZSetOperations<String, String> zsetOperations;
    
    @Resource(name="redisTemplate")
    protected ValueOperations<String, String> valueOperations;
    
    //---------------------------------------------------------------------
    // ZSetOperations -> Redis Sort Set 操作
    //---------------------------------------------------------------------
    
    /**
     * 设置zset值
     * @param key
     * @param member
     * @param score
     * @author lianggz
     */
    public boolean addZSetValue(String key, String member, long score){
        return zsetOperations.add(key, member, score);
    }
    
    /**
     * 设置zset值
     * @param key
     * @param member
     * @param score
     * @author lianggz
     */
    public boolean addZSetValue(String key, String member, double score){
        return zsetOperations.add(key, member, score);
    }
    
    /**
     * 批量设置zset值
     * @param key
     * @param member
     * @param score
     * @author lianggz
     */
    public long addBatchZSetValue(String key, Set<TypedTuple<String>> tuples){
        return zsetOperations.add(key, tuples);
    }
    
    /**
     * 自增zset值
     * @param key
     * @param member
     * @param delta
     * @author lianggz
     */
    public void incZSetValue(String key, String member, long delta){
        zsetOperations.incrementScore(key, member, delta);
    }
    
    /**
     * 获取zset数量
     * @param key
     * @param member
     * @return
     * @author lianggz
     */
    public long getZSetScore(String key, String member){
        Double score = zsetOperations.score(key, member);
        if(score==null){
            return 0;
        }else{
            return score.longValue();   
        }       
    }
    
    /**
     * 获取zset数量
     * @param key
     * @param member
     * @return
     * @author lianggz
     */
    public double getZSetScore4Double(String key, String member){
        Double score = zsetOperations.score(key, member);
        if(score==null){
            return 0;
        }else{
            return score;   
        }       
    }
    
    /**
     * 删除zset值
     * @param key
     * @param member
     * @param score
     * @author lianggz
     */
    /*public boolean deleteZSetValue(String key, String member){
        return zsetOperations.remove(key, member);
    }*/
    
    /**
     * 获取有序集 key 中成员 member 的排名 。其中有序集成员按 score 值递减 (从小到大) 排序。     
     * @param key
     * @param start
     * @param end
     * @return
     * @author lianggz
     */
    public Set<TypedTuple<String>> getZSetRank(String key, long start, long end){
        return zsetOperations.rangeWithScores(key, start, end);
    }
    
    /**
     * 获取有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减 (从大到小) 排序。      
     * @param key
     * @param start
     * @param end
     * @return
     * @author lianggz
     */
    public Set<TypedTuple<String>> getZSetRevRank(String key, long start, long end){
        return zsetOperations.reverseRangeWithScores(key, start, end);
    }
    
    /**
     * 获取zset数量   
     * @param key
     * @return
     * @author lianggz
     */
    public long getZSetCount(String key){
        return zsetOperations.size(key);
    }
    
    //---------------------------------------------------------------------
    // HashOperations -> Redis Redis Hash 操作
    //---------------------------------------------------------------------
    
    /** 
     * 添加单个数据         
     * @param key
     * @param hashKey
     * @param value
     * @author lianggz
     */
    public void addHashValue(String key, String hashKey, String value) {    
        hashOperations.put(key, hashKey, value);
    }
    
    /** 
     * 添加单个数据         
     * @param key
     * @param hashKey
     * @param value
     * @author lianggz
     */
    public void addBatchHashValue(String key, Map<String, String> m) {    
        hashOperations.putAll(key, m);
    }
    
    /**
     * 自增hash值      
     * @param key
     * @param hashKey
     * @param delta
     * @author lianggz
     */
    public void incHashValue(String key, String hashKey, long delta) {
        hashOperations.increment(key, hashKey, delta);
    }
    
    /** 
     * 删除单个数据         
     * @param key
     * @param hashKey
     * @author lianggz
     */
    public void deleteHashValue(String key, String hashKey) {    
        hashOperations.delete(key, hashKey);
    }
    
    /** 
     * 获取单个数据       
     * @param key
     * @param hashKey
     * @return
     * @author lianggz
     */
    public String getHashValue(String key, String hashKey) {
        return hashOperations.get(key, hashKey);      
    } 


    /**
     * 批量获取数据      
     * @param key
     * @return
     * @author lianggz
     */
    public List<String> getHashAllValue(String key) {
        List<String> values = hashOperations.values(key);
        return values;
    }
    
    /**
     * 批量获取 数据      
     * @param key
     * @param hashKeys
     * @return
     * @author lianggz
     */
    public List<String> getHashMultiValue(String key, List<String> hashKeys) {
        List<String> values = hashOperations.multiGet(key, hashKeys);
        return values;
    }
    
    /**
     * 获取hash数量       
     * @param key
     * @return
     * @author lianggz
     */
    public Long getHashCount(String key) {
        return hashOperations.size(key);
    }
    
    //---------------------------------------------------------------------
    // ValueOperations -> Redis String/Value 操作
    //---------------------------------------------------------------------
    
    /**
     * 设置value值
     * @param key
     * @param member
     * @author lianggz
     */
    public void addValue(String key, String value){
        valueOperations.set(key, value);
    }
    
    /**
     * 获取value值
     * @param key
     * @author lianggz
     */
    public String getValue(String key){
        return valueOperations.get(key);
    }
    
    //---------------------------------------------------------------------
    // redisTemplate
    //---------------------------------------------------------------------
    
    /**
     * 是否存在    
     * @param key
     * @return
     * @author lianggz
     */
    public boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }

    /** 
     * 是否存在      
     * @param key
     * @param hashKey
     * @return
     * @author lianggz
     */
    public boolean hasKey(String key, String hashKey) {
        return redisTemplate.opsForHash().hasKey(key, hashKey);
    }
    
    /** 
     * 设置超时      
     * @param key
     * @param timeout
     * @param unit
     * @author lianggz
     */
    public void expire(String key, final long timeout, final TimeUnit unit) {
        redisTemplate.expire(key, timeout, unit);
    }
    
    /** 
     * 所有keys      
     * @param pattern
     * @author lianggz
     */
    public Set<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }
    
    /** 
     * 删除keys      
     * @param key
     * @author lianggz
     */
    public void delete(Set<String> keys) {
        redisTemplate.delete(keys);
    }
}
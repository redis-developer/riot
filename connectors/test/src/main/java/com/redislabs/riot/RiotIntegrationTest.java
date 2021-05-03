package com.redislabs.riot;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisStandaloneContainer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Testcontainers
public abstract class RiotIntegrationTest extends RiotTest {

    @Container
    private static final RedisStandaloneContainer REDIS = new RedisStandaloneContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();

    protected static final Map<RedisContainer, AbstractRedisClient> CLIENTS = new HashMap<>();
    protected static final Map<RedisContainer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
    protected static final Map<RedisContainer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
    protected static final Map<RedisContainer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
    protected static final Map<RedisContainer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisContainer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    @BeforeAll
    public static void setup() {
        add(REDIS);
        add(REDIS_CLUSTER);
    }

    private static void add(RedisContainer container) {
        if (container instanceof RedisClusterContainer) {
            RedisClusterClient client = RedisClusterClient.create(container.getRedisURI());
            CLIENTS.put(container, client);
            StatefulRedisClusterConnection<String, String> connection = client.connect();
            CONNECTIONS.put(container, connection);
            SYNCS.put(container, connection.sync());
            ASYNCS.put(container, connection.async());
            PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
            POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
        } else {
            RedisClient client = RedisClient.create(container.getRedisURI());
            CLIENTS.put(container, client);
            StatefulRedisConnection<String, String> connection = client.connect();
            CONNECTIONS.put(container, connection);
            SYNCS.put(container, connection.sync());
            ASYNCS.put(container, connection.async());
            PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
            POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
        }
    }

    @AfterEach
    public void flushall() {
        for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
    }

    @AfterAll
    public static void teardown() {
        CONNECTIONS.values().forEach(RedisOptions::close);
        PUBSUB_CONNECTIONS.values().forEach(RedisOptions::close);
        POOLS.values().forEach(RedisOptions::close);
        CLIENTS.values().forEach(RedisOptions::shutdown);
        SYNCS.clear();
        ASYNCS.clear();
        CONNECTIONS.clear();
        PUBSUB_CONNECTIONS.clear();
        POOLS.clear();
        CLIENTS.clear();
    }

    static Stream<RedisContainer> containers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    static Stream<RedisContainer> standaloneContainer() {
        return Stream.of(REDIS);
    }

    protected static <T> T sync(RedisContainer container) {
        return (T) SYNCS.get(container);
    }

    protected static <T> T async(RedisContainer container) {
        return (T) ASYNCS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> C connection(RedisContainer container) {
        return (C) CONNECTIONS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisContainer container) {
        return (GenericObjectPool<C>) POOLS.get(container);
    }

    protected DataGenerator.DataGeneratorBuilder dataGenerator(RedisContainer container) {
        return DataGenerator.builder(connection(container));
    }


}

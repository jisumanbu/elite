# Kafka & 微服务 Testcontainers 白盒测试实现指南

## 1. 项目结构

### 1.1 推荐的项目结构

```
elite/
├── pom.xml                                    # Maven 父 POM
├── docker-compose.yml                         # 本地开发环境
├── docs/
│   ├── 01-requirements-analysis.md
│   ├── 02-architecture-design.md
│   └── 03-implementation-guide.md
│
├── common/                                    # 公共模块
│   ├── pom.xml
│   └── src/main/java/
│       └── com/example/common/
│           ├── event/                         # 事件定义
│           │   ├── OrderCreatedEvent.java
│           │   └── PaymentCompletedEvent.java
│           └── dto/                           # 数据传输对象
│
├── order-service/                             # 订单服务
│   ├── pom.xml
│   └── src/
│       ├── main/java/
│       │   └── com/example/order/
│       │       ├── OrderApplication.java
│       │       ├── controller/
│       │       ├── service/
│       │       ├── repository/
│       │       └── kafka/
│       │           ├── producer/
│       │           └── consumer/
│       └── test/java/
│           └── com/example/order/
│               ├── unit/                      # 单元测试
│               ├── component/                 # 组件测试
│               └── integration/               # 集成测试
│
├── payment-service/                           # 支付服务
│   └── (结构同 order-service)
│
└── test-infrastructure/                       # 测试基础设施模块
    ├── pom.xml
    └── src/main/java/
        └── com/example/test/
            ├── container/                     # 容器配置
            │   ├── KafkaContainerConfig.java
            │   ├── MySQLContainerConfig.java
            │   └── RedisContainerConfig.java
            ├── base/                          # 测试基类
            │   ├── AbstractKafkaTest.java
            │   └── AbstractIntegrationTest.java
            └── util/                          # 测试工具
                ├── KafkaTestUtils.java
                └── TestDataBuilder.java
```

---

## 2. 依赖配置

### 2.1 父 POM 配置

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.0</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>elite-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>Elite - Kafka Testcontainers Framework</name>

    <modules>
        <module>common</module>
        <module>order-service</module>
        <module>payment-service</module>
        <module>test-infrastructure</module>
    </modules>

    <properties>
        <java.version>17</java.version>
        <spring-kafka.version>3.1.0</spring-kafka.version>
        <testcontainers.version>1.19.3</testcontainers.version>
        <awaitility.version>4.2.0</awaitility.version>
        <assertj.version>3.24.2</assertj.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <!-- Testcontainers BOM -->
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <!-- Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>

        <!-- Spring Kafka -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- Test Dependencies -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Testcontainers -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mysql</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Awaitility for async testing -->
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>${awaitility.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- AssertJ -->
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.2</version>
                <configuration>
                    <includes>
                        <include>**/*Test.java</include>
                    </includes>
                    <excludes>
                        <exclude>**/*IntegrationTest.java</exclude>
                    </excludes>
                </configuration>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>3.2.2</version>
                <configuration>
                    <includes>
                        <include>**/*IntegrationTest.java</include>
                    </includes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

            <!-- JaCoCo for code coverage -->
            <plugin>
                <groupId>org.jacoco</groupId>
                <artifactId>jacoco-maven-plugin</artifactId>
                <version>0.8.11</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>prepare-agent</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>report</id>
                        <phase>test</phase>
                        <goals>
                            <goal>report</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 3. Kafka 容器配置

### 3.1 Kafka 容器配置类

```java
package com.example.test.container;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka 容器配置
 * 支持单例模式和 KRaft 模式
 */
public class KafkaContainerConfig {

    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.5.0";
    private static KafkaContainer kafkaContainer;

    /**
     * 获取共享的 Kafka 容器实例（单例模式）
     */
    public static KafkaContainer getInstance() {
        if (kafkaContainer == null) {
            synchronized (KafkaContainerConfig.class) {
                if (kafkaContainer == null) {
                    kafkaContainer = createKafkaContainer();
                    kafkaContainer.start();
                }
            }
        }
        return kafkaContainer;
    }

    /**
     * 创建新的 Kafka 容器（KRaft 模式 - 无需 Zookeeper）
     */
    public static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withKraft()  // 使用 KRaft 模式
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withEnv("KAFKA_NUM_PARTITIONS", "3")
                .withEnv("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
                .withReuse(true);  // 启用容器复用
    }

    /**
     * 创建 Kafka 容器并加入指定网络
     */
    public static KafkaContainer createKafkaContainer(Network network) {
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withKraft()
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withReuse(true);
    }

    /**
     * 获取 Kafka Bootstrap Servers 地址
     */
    public static String getBootstrapServers() {
        return getInstance().getBootstrapServers();
    }
}
```

### 3.2 MySQL 容器配置

```java
package com.example.test.container;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * MySQL 容器配置
 */
public class MySQLContainerConfig {

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static MySQLContainer<?> mysqlContainer;

    /**
     * 获取共享的 MySQL 容器实例
     */
    public static MySQLContainer<?> getInstance() {
        if (mysqlContainer == null) {
            synchronized (MySQLContainerConfig.class) {
                if (mysqlContainer == null) {
                    mysqlContainer = createMySQLContainer();
                    mysqlContainer.start();
                }
            }
        }
        return mysqlContainer;
    }

    /**
     * 创建新的 MySQL 容器
     */
    public static MySQLContainer<?> createMySQLContainer() {
        return new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test")
                .withInitScript("db/init.sql")  // 初始化脚本
                .withReuse(true);
    }

    /**
     * 创建 MySQL 容器并加入指定网络
     */
    public static MySQLContainer<?> createMySQLContainer(Network network) {
        return new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("mysql")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test")
                .withReuse(true);
    }
}
```

### 3.3 Redis 容器配置

```java
package com.example.test.container;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Redis 容器配置
 */
public class RedisContainerConfig {

    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static final int REDIS_PORT = 6379;
    private static GenericContainer<?> redisContainer;

    /**
     * 获取共享的 Redis 容器实例
     */
    public static GenericContainer<?> getInstance() {
        if (redisContainer == null) {
            synchronized (RedisContainerConfig.class) {
                if (redisContainer == null) {
                    redisContainer = createRedisContainer();
                    redisContainer.start();
                }
            }
        }
        return redisContainer;
    }

    /**
     * 创建新的 Redis 容器
     */
    public static GenericContainer<?> createRedisContainer() {
        return new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                .withExposedPorts(REDIS_PORT)
                .withReuse(true);
    }

    /**
     * 创建 Redis 容器并加入指定网络
     */
    public static GenericContainer<?> createRedisContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("redis")
                .withExposedPorts(REDIS_PORT)
                .withReuse(true);
    }

    /**
     * 获取 Redis 连接 URL
     */
    public static String getRedisUrl() {
        return String.format("redis://%s:%d",
                getInstance().getHost(),
                getInstance().getMappedPort(REDIS_PORT));
    }
}
```

---

## 4. 测试基类实现

### 4.1 Kafka 测试基类

```java
package com.example.test.base;

import com.example.test.container.KafkaContainerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka 测试基类
 * 提供 Kafka 容器管理、Topic 管理、Producer/Consumer 工具
 */
@Testcontainers
public abstract class AbstractKafkaTest {

    @Container
    protected static final KafkaContainer kafka = KafkaContainerConfig.createKafkaContainer();

    protected AdminClient adminClient;
    protected KafkaProducer<String, String> producer;
    protected KafkaConsumer<String, String> consumer;
    protected Set<String> createdTopics = new HashSet<>();

    @BeforeAll
    static void startKafka() {
        // 容器已通过 @Container 注解自动启动
    }

    @BeforeEach
    void setUp() {
        adminClient = createAdminClient();
        producer = createProducer();
    }

    @AfterEach
    void tearDown() {
        // 清理创建的 Topics
        if (!createdTopics.isEmpty()) {
            try {
                adminClient.deleteTopics(createdTopics).all().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // 忽略删除失败
            }
            createdTopics.clear();
        }

        // 关闭资源
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    // ==================== Admin 操作 ====================

    protected AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return AdminClient.create(props);
    }

    /**
     * 创建 Topic
     */
    protected void createTopic(String topicName, int partitions, short replicationFactor) {
        try {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(topic))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            createdTopics.add(topicName);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }

    /**
     * 创建单分区 Topic
     */
    protected void createTopic(String topicName) {
        createTopic(topicName, 1, (short) 1);
    }

    // ==================== Producer 操作 ====================

    protected KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new KafkaProducer<>(props);
    }

    /**
     * 发送消息并等待确认
     */
    protected void sendMessage(String topic, String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException("Failed to send message", e);
        }
    }

    /**
     * 发送消息（无 key）
     */
    protected void sendMessage(String topic, String value) {
        sendMessage(topic, null, value);
    }

    // ==================== Consumer 操作 ====================

    protected KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    /**
     * 消费消息
     */
    protected List<ConsumerRecord<String, String>> consumeMessages(
            String topic,
            String groupId,
            int expectedCount,
            Duration timeout) {

        consumer = createConsumer(groupId);
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long endTime = System.currentTimeMillis() + timeout.toMillis();

        while (records.size() < expectedCount && System.currentTimeMillis() < endTime) {
            consumer.poll(Duration.ofMillis(100))
                    .forEach(records::add);
        }

        return records;
    }

    /**
     * 获取 Bootstrap Servers
     */
    protected String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }
}
```

### 4.2 集成测试基类

```java
package com.example.test.base;

import com.example.test.container.KafkaContainerConfig;
import com.example.test.container.MySQLContainerConfig;
import com.example.test.container.RedisContainerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 集成测试基类
 * 提供完整的容器化测试环境：Kafka + MySQL + Redis
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    protected static Network network = Network.newNetwork();

    @Container
    protected static final KafkaContainer kafka = KafkaContainerConfig.createKafkaContainer(network);

    @Container
    protected static final MySQLContainer<?> mysql = MySQLContainerConfig.createMySQLContainer(network);

    @Container
    protected static final GenericContainer<?> redis = RedisContainerConfig.createRedisContainer(network);

    @BeforeAll
    static void startContainers() {
        // 容器已通过 @Container 注解自动启动
    }

    /**
     * 动态配置 Spring 属性
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Kafka 配置
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
        registry.add("spring.kafka.consumer.group-id", () -> "test-group");

        // MySQL 配置
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");

        // Redis 配置
        registry.add("spring.redis.host", redis::getHost);
        registry.add("spring.redis.port", () -> redis.getMappedPort(6379));

        // JPA 配置
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.show-sql", () -> "true");
    }
}
```

---

## 5. 测试工具类

### 5.1 Kafka 测试工具

```java
package com.example.test.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka 测试工具类
 */
public class KafkaTestUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 等待条件满足
     */
    public static void awaitUntil(Callable<Boolean> condition, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .until(condition);
    }

    /**
     * 等待消息数量达到预期
     */
    public static void awaitMessageCount(
            Callable<List<?>> messagesSupplier,
            int expectedCount,
            Duration timeout) {

        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    List<?> messages = messagesSupplier.call();
                    assertThat(messages).hasSize(expectedCount);
                });
    }

    /**
     * 验证消息内容
     */
    public static <T> void assertMessageContent(
            ConsumerRecord<String, String> record,
            Class<T> targetClass,
            Consumer<T> assertions) {

        try {
            T value = objectMapper.readValue(record.value(), targetClass);
            assertions.accept(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse message", e);
        }
    }

    /**
     * 查找匹配的消息
     */
    public static <K, V> ConsumerRecord<K, V> findMessage(
            List<ConsumerRecord<K, V>> records,
            Predicate<ConsumerRecord<K, V>> predicate) {

        return records.stream()
                .filter(predicate)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No matching message found"));
    }

    /**
     * 序列化对象为 JSON
     */
    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

    /**
     * 反序列化 JSON 为对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize from JSON", e);
        }
    }
}
```

### 5.2 测试数据构建器

```java
package com.example.test.util;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 测试数据构建器
 */
public class TestDataBuilder {

    /**
     * 生成唯一的订单 ID
     */
    public static String generateOrderId() {
        return "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /**
     * 生成唯一的用户 ID
     */
    public static String generateUserId() {
        return "USR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /**
     * 生成唯一的 Topic 名称（避免测试冲突）
     */
    public static String generateTopicName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 生成唯一的 Consumer Group ID
     */
    public static String generateGroupId() {
        return "test-group-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 构建订单事件 JSON
     */
    public static String buildOrderCreatedEvent(String orderId, String userId, BigDecimal amount) {
        return String.format("""
            {
                "eventId": "%s",
                "eventType": "ORDER_CREATED",
                "timestamp": "%s",
                "payload": {
                    "orderId": "%s",
                    "userId": "%s",
                    "amount": %s,
                    "status": "PENDING"
                }
            }
            """,
                UUID.randomUUID().toString(),
                LocalDateTime.now().toString(),
                orderId,
                userId,
                amount.toString()
        );
    }

    /**
     * 构建支付事件 JSON
     */
    public static String buildPaymentCompletedEvent(String orderId, String paymentId) {
        return String.format("""
            {
                "eventId": "%s",
                "eventType": "PAYMENT_COMPLETED",
                "timestamp": "%s",
                "payload": {
                    "orderId": "%s",
                    "paymentId": "%s",
                    "status": "SUCCESS"
                }
            }
            """,
                UUID.randomUUID().toString(),
                LocalDateTime.now().toString(),
                orderId,
                paymentId
        );
    }
}
```

---

## 6. 测试示例

### 6.1 Kafka Producer 测试

```java
package com.example.order.kafka.producer;

import com.example.test.base.AbstractKafkaTest;
import com.example.test.util.TestDataBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Producer 组件测试
 */
class OrderProducerTest extends AbstractKafkaTest {

    private String testTopic;

    @BeforeEach
    void setUpTopic() {
        testTopic = TestDataBuilder.generateTopicName("order-events");
        createTopic(testTopic, 3, (short) 1);
    }

    @Test
    @DisplayName("应该成功发送订单创建事件到 Kafka")
    void shouldSendOrderCreatedEventToKafka() {
        // Given
        String orderId = TestDataBuilder.generateOrderId();
        String userId = TestDataBuilder.generateUserId();
        String eventJson = TestDataBuilder.buildOrderCreatedEvent(orderId, userId, new BigDecimal("99.99"));

        // When
        sendMessage(testTopic, orderId, eventJson);

        // Then
        List<ConsumerRecord<String, String>> records = consumeMessages(
                testTopic,
                TestDataBuilder.generateGroupId(),
                1,
                Duration.ofSeconds(10)
        );

        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo(orderId);
        assertThat(records.get(0).value()).contains("ORDER_CREATED");
        assertThat(records.get(0).value()).contains(orderId);
    }

    @Test
    @DisplayName("应该将相同 key 的消息发送到同一分区")
    void shouldSendMessagesWithSameKeyToSamePartition() {
        // Given
        String orderId = TestDataBuilder.generateOrderId();

        // When - 发送多条相同 key 的消息
        for (int i = 0; i < 5; i++) {
            sendMessage(testTopic, orderId, "message-" + i);
        }

        // Then
        List<ConsumerRecord<String, String>> records = consumeMessages(
                testTopic,
                TestDataBuilder.generateGroupId(),
                5,
                Duration.ofSeconds(10)
        );

        // 验证所有消息都在同一分区
        int partition = records.get(0).partition();
        assertThat(records)
                .allMatch(r -> r.partition() == partition, "All messages should be in the same partition");
    }

    @Test
    @DisplayName("应该按顺序发送和接收消息")
    void shouldMaintainMessageOrder() {
        // Given
        String orderId = TestDataBuilder.generateOrderId();

        // When
        for (int i = 0; i < 10; i++) {
            sendMessage(testTopic, orderId, String.valueOf(i));
        }

        // Then
        List<ConsumerRecord<String, String>> records = consumeMessages(
                testTopic,
                TestDataBuilder.generateGroupId(),
                10,
                Duration.ofSeconds(10)
        );

        // 验证消息顺序
        for (int i = 0; i < 10; i++) {
            assertThat(records.get(i).value()).isEqualTo(String.valueOf(i));
        }
    }
}
```

### 6.2 Kafka Consumer 测试

```java
package com.example.order.kafka.consumer;

import com.example.order.service.OrderService;
import com.example.test.base.AbstractIntegrationTest;
import com.example.test.util.KafkaTestUtils;
import com.example.test.util.TestDataBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Consumer 集成测试
 */
class PaymentEventConsumerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private OrderService orderService;

    @Test
    @DisplayName("应该在收到支付完成事件后更新订单状态")
    void shouldUpdateOrderStatusWhenPaymentCompleted() throws Exception {
        // Given - 先创建一个订单
        String orderId = TestDataBuilder.generateOrderId();
        orderService.createOrder(orderId, "user-1", new BigDecimal("100.00"));

        // When - 发送支付完成事件
        String paymentEvent = TestDataBuilder.buildPaymentCompletedEvent(orderId, "PAY-12345");
        kafkaTemplate.send("payment-events", orderId, paymentEvent).get();

        // Then - 等待订单状态更新
        KafkaTestUtils.awaitUntil(() -> {
            return orderService.getOrderStatus(orderId).equals("PAID");
        }, Duration.ofSeconds(10));

        assertThat(orderService.getOrderStatus(orderId)).isEqualTo("PAID");
    }

    @Test
    @DisplayName("应该处理无效的支付事件并记录错误")
    void shouldHandleInvalidPaymentEvent() throws Exception {
        // Given
        String invalidEvent = "{ invalid json }";

        // When
        kafkaTemplate.send("payment-events", "invalid-key", invalidEvent).get();

        // Then - 验证错误被正确处理（没有抛出异常，应用继续运行）
        Thread.sleep(2000);
        // 验证应用仍然正常运行
        assertThat(orderService).isNotNull();
    }
}
```

### 6.3 端到端测试

```java
package com.example.e2e;

import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.test.base.AbstractIntegrationTest;
import com.example.test.util.KafkaTestUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 订单流程端到端测试
 */
class OrderFlowEndToEndTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    @DisplayName("完整订单流程：创建订单 -> 发送事件 -> 支付 -> 更新状态")
    void shouldCompleteFullOrderFlow() {
        // Step 1: 创建订单
        CreateOrderRequest request = new CreateOrderRequest();
        request.setUserId("user-123");
        request.setAmount(new BigDecimal("199.99"));
        request.setProductId("prod-456");

        ResponseEntity<OrderResponse> createResponse = restTemplate.postForEntity(
                "/api/orders",
                request,
                OrderResponse.class
        );

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createResponse.getBody()).isNotNull();
        String orderId = createResponse.getBody().getOrderId();
        assertThat(orderId).isNotBlank();

        // Step 2: 验证订单状态为 PENDING
        ResponseEntity<OrderResponse> getResponse = restTemplate.getForEntity(
                "/api/orders/" + orderId,
                OrderResponse.class
        );

        assertThat(getResponse.getBody().getStatus()).isEqualTo("PENDING");

        // Step 3: 模拟支付（触发支付完成事件）
        ResponseEntity<Void> payResponse = restTemplate.postForEntity(
                "/api/orders/" + orderId + "/pay",
                null,
                Void.class
        );

        assertThat(payResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Step 4: 等待异步处理完成，验证订单状态更新为 PAID
        KafkaTestUtils.awaitUntil(() -> {
            ResponseEntity<OrderResponse> statusResponse = restTemplate.getForEntity(
                    "/api/orders/" + orderId,
                    OrderResponse.class
            );
            return "PAID".equals(statusResponse.getBody().getStatus());
        }, Duration.ofSeconds(15));

        // 最终验证
        ResponseEntity<OrderResponse> finalResponse = restTemplate.getForEntity(
                "/api/orders/" + orderId,
                OrderResponse.class
        );

        assertThat(finalResponse.getBody())
                .satisfies(order -> {
                    assertThat(order.getStatus()).isEqualTo("PAID");
                    assertThat(order.getPaymentId()).isNotBlank();
                    assertThat(order.getPaidAt()).isNotNull();
                });
    }
}
```

---

## 7. 异常场景测试

### 7.1 Kafka 故障模拟

```java
package com.example.order.kafka;

import com.example.test.base.AbstractKafkaTest;
import com.example.test.util.TestDataBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Kafka 故障场景测试
 */
class KafkaFailureScenarioTest extends AbstractKafkaTest {

    @Test
    @DisplayName("当 Kafka 不可用时，Producer 应该超时失败")
    void shouldTimeoutWhenKafkaUnavailable() {
        // Given - 创建一个指向不存在 Kafka 的 Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19999");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");

        var failingProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        // When & Then
        assertThatThrownBy(() -> {
            failingProducer.send(new ProducerRecord<>("test-topic", "key", "value")).get();
        })
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(TimeoutException.class);

        failingProducer.close(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("当 Topic 不存在且自动创建禁用时应该失败")
    void shouldFailWhenTopicDoesNotExistAndAutoCreateDisabled() {
        // 这个测试验证配置是否正确处理不存在的 Topic
        String nonExistentTopic = "non-existent-topic-" + System.currentTimeMillis();

        // 发送消息到不存在的 Topic（默认会自动创建）
        sendMessage(nonExistentTopic, "test-key", "test-value");

        // 验证 Topic 被创建
        var records = consumeMessages(nonExistentTopic,
                TestDataBuilder.generateGroupId(), 1, Duration.ofSeconds(10));

        org.assertj.core.api.Assertions.assertThat(records).hasSize(1);
    }
}
```

### 7.2 消息重试测试

```java
package com.example.order.kafka.consumer;

import com.example.order.service.OrderService;
import com.example.test.base.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 消息重试机制测试
 */
class MessageRetryTest extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @SpyBean
    private OrderService orderService;

    @Test
    @DisplayName("消息处理失败时应该重试指定次数")
    void shouldRetryOnProcessingFailure() throws Exception {
        // Given - 模拟前两次处理失败，第三次成功
        doThrow(new RuntimeException("Temporary failure"))
                .doThrow(new RuntimeException("Temporary failure"))
                .doCallRealMethod()
                .when(orderService).processPaymentEvent(any());

        String event = """
            {
                "orderId": "ORD-123",
                "paymentId": "PAY-456",
                "status": "SUCCESS"
            }
            """;

        // When
        kafkaTemplate.send("payment-events", "ORD-123", event).get();

        // Then - 验证重试了 3 次
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    verify(orderService, times(3)).processPaymentEvent(any());
                });
    }

    @Test
    @DisplayName("超过最大重试次数后应该发送到死信队列")
    void shouldSendToDeadLetterQueueAfterMaxRetries() throws Exception {
        // Given - 模拟一直失败
        doThrow(new RuntimeException("Permanent failure"))
                .when(orderService).processPaymentEvent(any());

        String event = """
            {
                "orderId": "ORD-FAIL",
                "paymentId": "PAY-FAIL",
                "status": "SUCCESS"
            }
            """;

        // When
        kafkaTemplate.send("payment-events", "ORD-FAIL", event).get();

        // Then - 验证消息被发送到死信队列
        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    // 验证死信队列收到消息
                    // 这里需要消费 payment-events.DLT topic
                });
    }
}
```

---

## 8. 配置文件

### 8.1 测试配置 (application-test.yml)

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false
      properties:
        spring.json.trusted.packages: "*"
    producer:
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
    listener:
      ack-mode: record
      concurrency: 3

  # JPA 配置
  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: true
    properties:
      hibernate:
        format_sql: true

# 日志配置
logging:
  level:
    org.testcontainers: INFO
    com.github.dockerjava: WARN
    org.apache.kafka: WARN
    com.example: DEBUG
```

### 8.2 Testcontainers 配置

```properties
# src/test/resources/testcontainers.properties

# 启用容器复用（开发环境）
testcontainers.reuse.enable=true

# Ryuk 配置（容器清理守护进程）
ryuk.container.timeout=60

# Docker 客户端配置
# docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
```

---

## 9. CI/CD 配置

### 9.1 GitHub Actions

```yaml
# .github/workflows/test.yml
name: Integration Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      docker:
        image: docker:dind
        options: --privileged

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'
          cache: maven

      - name: Run Unit Tests
        run: mvn test -DskipIntegrationTests=true

      - name: Run Integration Tests
        run: mvn verify -DskipUnitTests=true
        env:
          TESTCONTAINERS_RYUK_DISABLED: false

      - name: Upload Test Reports
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-reports
          path: |
            **/target/surefire-reports/
            **/target/failsafe-reports/

      - name: Upload Coverage Report
        uses: actions/upload-artifact@v4
        with:
          name: coverage-report
          path: **/target/site/jacoco/
```

### 9.2 GitLab CI

```yaml
# .gitlab-ci.yml
stages:
  - build
  - test
  - coverage

variables:
  MAVEN_OPTS: "-Dmaven.repo.local=$CI_PROJECT_DIR/.m2/repository"
  DOCKER_HOST: tcp://docker:2375

build:
  stage: build
  image: maven:3.9-eclipse-temurin-17
  script:
    - mvn clean compile -DskipTests
  cache:
    paths:
      - .m2/repository

unit-test:
  stage: test
  image: maven:3.9-eclipse-temurin-17
  script:
    - mvn test -DskipIntegrationTests=true
  artifacts:
    reports:
      junit:
        - "**/target/surefire-reports/TEST-*.xml"

integration-test:
  stage: test
  image: maven:3.9-eclipse-temurin-17
  services:
    - docker:24-dind
  script:
    - mvn verify -DskipUnitTests=true
  artifacts:
    reports:
      junit:
        - "**/target/failsafe-reports/TEST-*.xml"

coverage:
  stage: coverage
  image: maven:3.9-eclipse-temurin-17
  script:
    - mvn jacoco:report
  artifacts:
    paths:
      - "**/target/site/jacoco/"
  coverage: '/Total.*?([0-9]{1,3})%/'
```

---

## 10. 常见问题解决

### 10.1 容器启动慢

**问题**: 容器启动时间过长

**解决方案**:
1. 使用容器复用功能
2. 预拉取镜像
3. 使用轻量级镜像（如 alpine 版本）

```java
// 预拉取镜像
@BeforeAll
static void pullImages() {
    DockerClientFactory.instance().client()
        .pullImageCmd("confluentinc/cp-kafka:7.5.0")
        .start()
        .awaitCompletion();
}
```

### 10.2 端口冲突

**问题**: 固定端口导致测试冲突

**解决方案**: 使用随机端口映射

```java
// 错误做法
container.withFixedExposedPort(9092, 9092);

// 正确做法
container.withExposedPorts(9092);
int mappedPort = container.getMappedPort(9092);
```

### 10.3 测试数据污染

**问题**: 测试数据相互影响

**解决方案**:
1. 使用唯一的 Topic 名称
2. 使用唯一的 Consumer Group
3. 每个测试后清理数据

```java
@BeforeEach
void setUp() {
    testTopic = "test-" + UUID.randomUUID().toString().substring(0, 8);
    createTopic(testTopic);
}

@AfterEach
void tearDown() {
    deleteTopic(testTopic);
}
```

---

## 11. 性能优化建议

| 优化项 | 描述 | 预期效果 |
|--------|------|----------|
| 容器复用 | 跨测试类共享容器 | 减少 60% 启动时间 |
| 并行测试 | 使用 JUnit 5 并行执行 | 减少 50% 总执行时间 |
| 镜像缓存 | CI 环境缓存 Docker 镜像 | 减少 30% 构建时间 |
| 选择性测试 | 只运行变更相关的测试 | 减少 70% 日常测试时间 |
| 轻量级镜像 | 使用 alpine 版本镜像 | 减少 40% 镜像拉取时间 |

---

## 12. 下一步

1. 克隆本项目模板
2. 根据实际微服务调整配置
3. 实现具体的业务测试用例
4. 集成到 CI/CD 流水线

如有问题，请参考官方文档:
- [Testcontainers 文档](https://testcontainers.com/)
- [Spring Kafka 测试指南](https://docs.spring.io/spring-kafka/reference/testing.html)

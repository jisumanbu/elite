# Elite 系统测试实现指南

## 1. 项目结构

### 1.1 推荐的测试代码结构

```
elite/
├── pom.xml
├── src/
│   ├── main/java/com/elite/
│   │   ├── config/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── repository/
│   │   ├── kafka/
│   │   │   ├── producer/
│   │   │   │   └── OtcSubscriptionProducer.java
│   │   │   └── consumer/
│   │   │       └── PaymentEventConsumer.java
│   │   ├── entity/
│   │   │   ├── StpGenerateTicket.java
│   │   │   ├── StpGenerateTicketAudit.java
│   │   │   ├── StpTicket.java
│   │   │   └── PostTradeAction.java
│   │   └── websocket/
│   │       └── TradeStatusWebSocketHandler.java
│   │
│   └── test/java/com/elite/
│       ├── base/                              # 测试基础设施
│       │   ├── AbstractEliteTest.java         # 通用测试基类
│       │   ├── AbstractKafkaIntegrationTest.java
│       │   ├── AbstractDatabaseTest.java
│       │   └── TestContainersConfig.java      # 容器配置
│       │
│       ├── container/                         # 容器定义
│       │   ├── EliteKafkaContainer.java
│       │   ├── EliteMySQLContainer.java
│       │   └── EliteRedisContainer.java
│       │
│       ├── util/                              # 测试工具
│       │   ├── KafkaTestHelper.java
│       │   ├── DatabaseTestHelper.java
│       │   ├── WebSocketTestClient.java
│       │   └── TestDataFactory.java
│       │
│       ├── unit/                              # 单元测试
│       │   ├── service/
│       │   └── kafka/
│       │
│       ├── component/                         # 组件测试
│       │   ├── architecture1/                 # 架构1: 同步落地异步分发
│       │   │   ├── StpGenerateTicketTest.java
│       │   │   ├── OtcSubscriptionProducerTest.java
│       │   │   └── DownstreamConsumerTest.java
│       │   └── architecture2/                 # 架构2: 全异步 Payment
│       │       ├── PostTradeActionTest.java
│       │       ├── PaymentConsumerTest.java
│       │       └── WebSocketPushTest.java
│       │
│       ├── integration/                       # 集成测试
│       │   ├── Architecture1IntegrationTest.java
│       │   ├── Architecture2IntegrationTest.java
│       │   └── CrossArchitectureTest.java
│       │
│       ├── e2e/                               # 端到端测试
│       │   ├── TradeLifecycleE2ETest.java
│       │   └── PaymentFlowE2ETest.java
│       │
│       └── chaos/                             # 故障注入测试
│           ├── KafkaFailureTest.java
│           ├── DatabaseFailureTest.java
│           └── PaymentApiFailureTest.java
│
└── src/test/resources/
    ├── application-test.yml
    ├── testcontainers.properties
    ├── db/
    │   ├── schema.sql
    │   └── test-data.sql
    └── wiremock/
        └── mappings/
            └── payment-api.json
```

---

## 2. 基础设施代码

### 2.1 容器配置类

```java
package com.elite.test.container;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Elite 测试容器配置
 * 提供 Kafka、MySQL、Redis 容器的统一管理
 */
public class TestContainersConfig {

    private static final Network SHARED_NETWORK = Network.newNetwork();

    // ==================== Kafka Container ====================

    private static KafkaContainer kafkaContainer;

    public static KafkaContainer getKafkaContainer() {
        if (kafkaContainer == null) {
            synchronized (TestContainersConfig.class) {
                if (kafkaContainer == null) {
                    kafkaContainer = new KafkaContainer(
                            DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                            .withKraft()
                            .withNetwork(SHARED_NETWORK)
                            .withNetworkAliases("kafka")
                            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                            .withEnv("KAFKA_NUM_PARTITIONS", "3")
                            .withReuse(true);
                    kafkaContainer.start();
                }
            }
        }
        return kafkaContainer;
    }

    // ==================== MySQL Container ====================

    private static MySQLContainer<?> mysqlContainer;

    public static MySQLContainer<?> getMySQLContainer() {
        if (mysqlContainer == null) {
            synchronized (TestContainersConfig.class) {
                if (mysqlContainer == null) {
                    mysqlContainer = new MySQLContainer<>(
                            DockerImageName.parse("mysql:8.0"))
                            .withNetwork(SHARED_NETWORK)
                            .withNetworkAliases("mysql")
                            .withDatabaseName("elite_test")
                            .withUsername("elite")
                            .withPassword("elite123")
                            .withInitScript("db/schema.sql")
                            .withReuse(true);
                    mysqlContainer.start();
                }
            }
        }
        return mysqlContainer;
    }

    // ==================== Redis Container ====================

    private static GenericContainer<?> redisContainer;

    public static GenericContainer<?> getRedisContainer() {
        if (redisContainer == null) {
            synchronized (TestContainersConfig.class) {
                if (redisContainer == null) {
                    redisContainer = new GenericContainer<>(
                            DockerImageName.parse("redis:7-alpine"))
                            .withNetwork(SHARED_NETWORK)
                            .withNetworkAliases("redis")
                            .withExposedPorts(6379)
                            .withReuse(true);
                    redisContainer.start();
                }
            }
        }
        return redisContainer;
    }

    // ==================== Utility Methods ====================

    public static Network getSharedNetwork() {
        return SHARED_NETWORK;
    }

    public static String getKafkaBootstrapServers() {
        return getKafkaContainer().getBootstrapServers();
    }

    public static String getMySQLJdbcUrl() {
        return getMySQLContainer().getJdbcUrl();
    }

    public static String getRedisHost() {
        return getRedisContainer().getHost();
    }

    public static int getRedisPort() {
        return getRedisContainer().getMappedPort(6379);
    }
}
```

### 2.2 测试基类

```java
package com.elite.test.base;

import com.elite.test.container.TestContainersConfig;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Elite 集成测试基类
 * 提供完整的测试环境配置
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractEliteIntegrationTest {

    @BeforeAll
    static void startContainers() {
        // 确保所有容器已启动
        TestContainersConfig.getKafkaContainer();
        TestContainersConfig.getMySQLContainer();
        TestContainersConfig.getRedisContainer();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Kafka 配置
        registry.add("spring.kafka.bootstrap-servers",
                TestContainersConfig::getKafkaBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "elite-test-group");
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");

        // MySQL 配置
        registry.add("spring.datasource.url",
                TestContainersConfig::getMySQLJdbcUrl);
        registry.add("spring.datasource.username", () -> "elite");
        registry.add("spring.datasource.password", () -> "elite123");

        // Redis 配置
        registry.add("spring.redis.host", TestContainersConfig::getRedisHost);
        registry.add("spring.redis.port", TestContainersConfig::getRedisPort);

        // JPA 配置
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.jpa.show-sql", () -> "true");
    }
}
```

### 2.3 Kafka 测试基类

```java
package com.elite.test.base;

import com.elite.test.container.TestContainersConfig;
import com.elite.test.util.KafkaTestHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Kafka 组件测试基类
 */
public abstract class AbstractKafkaTest {

    protected KafkaTestHelper kafkaHelper;
    protected Set<String> createdTopics = new HashSet<>();

    @BeforeEach
    void setUpKafka() {
        kafkaHelper = new KafkaTestHelper(
                TestContainersConfig.getKafkaBootstrapServers());
    }

    @AfterEach
    void tearDownKafka() {
        // 清理创建的 Topics
        if (!createdTopics.isEmpty()) {
            kafkaHelper.deleteTopics(createdTopics);
            createdTopics.clear();
        }
        kafkaHelper.close();
    }

    /**
     * 创建测试 Topic
     */
    protected void createTopic(String topicName) {
        kafkaHelper.createTopic(topicName, 3, (short) 1);
        createdTopics.add(topicName);
    }

    /**
     * 发送消息
     */
    protected void sendMessage(String topic, String key, String value) {
        kafkaHelper.sendMessage(topic, key, value);
    }

    /**
     * 消费消息
     */
    protected List<ConsumerRecord<String, String>> consumeMessages(
            String topic, int expectedCount, Duration timeout) {
        return kafkaHelper.consumeMessages(topic, expectedCount, timeout);
    }
}
```

---

## 3. 测试工具类

### 3.1 Kafka 测试帮助类

```java
package com.elite.test.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Kafka 测试辅助工具类
 */
public class KafkaTestHelper implements AutoCloseable {

    private final String bootstrapServers;
    private final AdminClient adminClient;
    private final KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    public KafkaTestHelper(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.adminClient = createAdminClient();
        this.producer = createProducer();
    }

    // ==================== Admin Operations ====================

    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    public void createTopic(String topicName, int partitions, short replicationFactor) {
        try {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(topic))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }

    public void deleteTopics(Set<String> topics) {
        try {
            adminClient.deleteTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            // 忽略删除失败
        }
    }

    public boolean topicExists(String topicName) {
        try {
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (Exception e) {
            return false;
        }
    }

    // ==================== Producer Operations ====================

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value))
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send message", e);
        }
    }

    public void sendMessageAsync(String topic, String key, String value,
                                  Callback callback) {
        producer.send(new ProducerRecord<>(topic, key, value), callback);
    }

    // ==================== Consumer Operations ====================

    public List<ConsumerRecord<String, String>> consumeMessages(
            String topic, int expectedCount, Duration timeout) {

        consumer = createConsumer(UUID.randomUUID().toString());
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long endTime = System.currentTimeMillis() + timeout.toMillis();

        while (records.size() < expectedCount && System.currentTimeMillis() < endTime) {
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(100));
            polled.forEach(records::add);
        }

        return records;
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    // ==================== Await Utilities ====================

    /**
     * 等待 Topic 有消息
     */
    public void awaitMessages(String topic, int count, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumeMessages(topic, count, Duration.ofSeconds(1)).size() >= count);
    }

    @Override
    public void close() {
        if (consumer != null) consumer.close();
        if (producer != null) producer.close();
        if (adminClient != null) adminClient.close();
    }
}
```

### 3.2 数据库测试帮助类

```java
package com.elite.test.util;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 数据库测试辅助工具类
 */
public class DatabaseTestHelper {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseTestHelper(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // ==================== stp_generate_ticket 操作 ====================

    /**
     * 插入 stp_generate_ticket 测试数据
     */
    public String insertStpGenerateTicket(String tradeId, String tradeData, String status) {
        String eventId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO stp_generate_ticket (event_id, trade_id, trade_data, status, created_at) " +
                "VALUES (?, ?, ?, ?, NOW())",
                eventId, tradeId, tradeData, status
        );
        return eventId;
    }

    /**
     * 查询 stp_generate_ticket 状态
     */
    public String getStpGenerateTicketStatus(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM stp_generate_ticket WHERE event_id = ?",
                String.class, eventId
        );
    }

    /**
     * 查询 PENDING 状态的记录数
     */
    public int countPendingStpGenerateTickets() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM stp_generate_ticket WHERE status = 'PENDING'",
                Integer.class
        );
    }

    // ==================== stp_generate_ticket_audit 操作 ====================

    /**
     * 检查审计记录是否存在
     */
    public boolean auditRecordExists(String eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM stp_generate_ticket_audit WHERE event_id = ?",
                Integer.class, eventId
        );
        return count != null && count > 0;
    }

    /**
     * 获取审计记录的 Kafka offset
     */
    public Long getAuditKafkaOffset(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT kafka_offset FROM stp_generate_ticket_audit WHERE event_id = ?",
                Long.class, eventId
        );
    }

    // ==================== stp_ticket 操作 ====================

    /**
     * 检查 ticket 是否已生成
     */
    public boolean ticketExists(String eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM stp_ticket WHERE event_id = ?",
                Integer.class, eventId
        );
        return count != null && count > 0;
    }

    // ==================== post_trade_action 操作 ====================

    /**
     * 插入 post_trade_action 测试数据
     */
    public String insertPostTradeAction(String tradeId, String actionType) {
        String actionId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO post_trade_action (action_id, trade_id, action_type, status, created_at) " +
                "VALUES (?, ?, ?, 'PENDING', NOW())",
                actionId, tradeId, actionType
        );
        return actionId;
    }

    /**
     * 查询 post_trade_action 状态
     */
    public String getPostTradeActionStatus(String actionId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM post_trade_action WHERE action_id = ?",
                String.class, actionId
        );
    }

    /**
     * 获取 post_trade_action 的 payment_id
     */
    public String getPostTradeActionPaymentId(String actionId) {
        return jdbcTemplate.queryForObject(
                "SELECT payment_id FROM post_trade_action WHERE action_id = ?",
                String.class, actionId
        );
    }

    /**
     * 更新 post_trade_action 状态
     */
    public void updatePostTradeActionStatus(String actionId, String status, String paymentId) {
        jdbcTemplate.update(
                "UPDATE post_trade_action SET status = ?, payment_id = ?, updated_at = NOW() " +
                "WHERE action_id = ?",
                status, paymentId, actionId
        );
    }

    // ==================== 数据清理 ====================

    /**
     * 清理测试数据
     */
    public void cleanupTestData() {
        jdbcTemplate.update("DELETE FROM stp_ticket WHERE event_id LIKE 'test-%'");
        jdbcTemplate.update("DELETE FROM stp_generate_ticket_audit WHERE event_id LIKE 'test-%'");
        jdbcTemplate.update("DELETE FROM stp_generate_ticket WHERE event_id LIKE 'test-%'");
        jdbcTemplate.update("DELETE FROM post_trade_action WHERE action_id LIKE 'test-%'");
    }

    // ==================== 一致性检查 ====================

    /**
     * 检查架构1数据一致性
     * 返回不一致的记录数
     */
    public int checkArchitecture1Consistency() {
        // 检查 SENT 状态但无审计记录的数量
        Integer sentWithoutAudit = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM stp_generate_ticket t " +
                "LEFT JOIN stp_generate_ticket_audit a ON t.event_id = a.event_id " +
                "WHERE t.status = 'SENT' AND a.event_id IS NULL",
                Integer.class
        );

        // 检查 COMPLETED 状态但无 ticket 的数量
        Integer completedWithoutTicket = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM stp_generate_ticket t " +
                "LEFT JOIN stp_ticket k ON t.event_id = k.event_id " +
                "WHERE t.status = 'COMPLETED' AND k.ticket_id IS NULL",
                Integer.class
        );

        return (sentWithoutAudit != null ? sentWithoutAudit : 0) +
               (completedWithoutTicket != null ? completedWithoutTicket : 0);
    }

    /**
     * 检查架构2对账一致性
     */
    public List<Map<String, Object>> checkArchitecture2Consistency() {
        // 查询 SUCCESS 但无 payment_id 的记录
        return jdbcTemplate.queryForList(
                "SELECT * FROM post_trade_action " +
                "WHERE status = 'SUCCESS' AND payment_id IS NULL"
        );
    }
}
```

### 3.3 WebSocket 测试客户端

```java
package com.elite.test.util;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WebSocket 测试客户端
 */
public class WebSocketTestClient extends WebSocketClient {

    private final List<String> receivedMessages = new ArrayList<>();
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private CountDownLatch messageLatch;

    public WebSocketTestClient(String uri) throws Exception {
        super(new URI(uri));
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        connectionLatch.countDown();
    }

    @Override
    public void onMessage(String message) {
        receivedMessages.add(message);
        if (messageLatch != null) {
            messageLatch.countDown();
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        // 连接关闭
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }

    /**
     * 等待连接建立
     */
    public boolean awaitConnection(long timeout, TimeUnit unit) throws InterruptedException {
        return connectionLatch.await(timeout, unit);
    }

    /**
     * 等待指定数量的消息
     */
    public boolean awaitMessages(int count, long timeout, TimeUnit unit) throws InterruptedException {
        messageLatch = new CountDownLatch(count);
        return messageLatch.await(timeout, unit);
    }

    /**
     * 获取收到的所有消息
     */
    public List<String> getReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }

    /**
     * 清空收到的消息
     */
    public void clearMessages() {
        receivedMessages.clear();
    }

    /**
     * 订阅指定交易的状态更新
     */
    public void subscribeToTrade(String tradeId) {
        send("{\"action\": \"subscribe\", \"tradeId\": \"" + tradeId + "\"}");
    }
}
```

### 3.4 测试数据工厂

```java
package com.elite.test.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 测试数据工厂
 * 生成 Equity Swap 相关的测试数据
 */
public class TestDataFactory {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 生成唯一的交易 ID
     */
    public static String generateTradeId() {
        return "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /**
     * 生成唯一的事件 ID
     */
    public static String generateEventId() {
        return "EVT-" + UUID.randomUUID().toString();
    }

    /**
     * 生成唯一的 Action ID
     */
    public static String generateActionId() {
        return "ACT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /**
     * 创建 Equity Swap 交易数据
     */
    public static String createEquitySwapTradeData(String tradeId) {
        Map<String, Object> tradeData = new HashMap<>();
        tradeData.put("tradeId", tradeId);
        tradeData.put("tradeType", "EQUITY_SWAP");
        tradeData.put("counterparty", "BANK-" + UUID.randomUUID().toString().substring(0, 4));
        tradeData.put("notionalAmount", new BigDecimal("10000000.00"));
        tradeData.put("currency", "USD");
        tradeData.put("underlyingAsset", "AAPL");
        tradeData.put("effectiveDate", LocalDate.now().toString());
        tradeData.put("maturityDate", LocalDate.now().plusYears(1).toString());
        tradeData.put("fixedRate", "0.025");
        tradeData.put("floatingRateIndex", "SOFR");
        tradeData.put("paymentFrequency", "QUARTERLY");

        try {
            return objectMapper.writeValueAsString(tradeData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize trade data", e);
        }
    }

    /**
     * 创建 STP Generate Ticket 事件消息
     */
    public static String createStpGenerateTicketEvent(String eventId, String tradeId, String tradeData) {
        Map<String, Object> event = new HashMap<>();
        event.put("eventId", eventId);
        event.put("eventType", "STP_GENERATE_TICKET");
        event.put("tradeId", tradeId);
        event.put("tradeData", tradeData);
        event.put("timestamp", System.currentTimeMillis());

        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

    /**
     * 创建 Post Trade Action (Payment) 事件消息
     */
    public static String createPostTradeActionEvent(String actionId, String tradeId, String actionType) {
        Map<String, Object> event = new HashMap<>();
        event.put("actionId", actionId);
        event.put("tradeId", tradeId);
        event.put("actionType", actionType);
        event.put("timestamp", System.currentTimeMillis());

        // Payment 相关参数
        Map<String, Object> paymentParams = new HashMap<>();
        paymentParams.put("amount", new BigDecimal("50000.00"));
        paymentParams.put("currency", "USD");
        paymentParams.put("paymentDate", LocalDate.now().plusDays(2).toString());
        paymentParams.put("beneficiary", "COUNTERPARTY-001");
        event.put("paymentParams", paymentParams);

        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

    /**
     * 创建 Payment API 成功响应
     */
    public static String createPaymentApiSuccessResponse(String paymentId) {
        Map<String, Object> response = new HashMap<>();
        response.put("paymentId", paymentId);
        response.put("status", "CREATED");
        response.put("createdAt", System.currentTimeMillis());

        try {
            return objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response", e);
        }
    }

    /**
     * 创建 Payment API 失败响应
     */
    public static String createPaymentApiErrorResponse(String errorCode, String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("error", errorCode);
        response.put("message", errorMessage);
        response.put("timestamp", System.currentTimeMillis());

        try {
            return objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response", e);
        }
    }
}
```

---

## 4. 架构1测试实现：同步落地·异步分发

### 4.1 STP Generate Ticket 组件测试

```java
package com.elite.test.component.architecture1;

import com.elite.entity.StpGenerateTicket;
import com.elite.repository.StpGenerateTicketRepository;
import com.elite.service.TradeService;
import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * STP Generate Ticket 组件测试
 * 测试架构1的同步落地阶段
 */
class StpGenerateTicketTest extends AbstractEliteIntegrationTest {

    @Autowired
    private TradeService tradeService;

    @Autowired
    private StpGenerateTicketRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
    }

    @Nested
    @DisplayName("TC1-001: 正常交易数据落地")
    class NormalTradeDataPersistence {

        @Test
        @DisplayName("应该成功创建 stp_generate_ticket 记录")
        void shouldCreateStpGenerateTicketRecord() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);

            // When
            StpGenerateTicket result = tradeService.createTrade(tradeId, tradeData);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getEventId()).isNotBlank();
            assertThat(result.getTradeId()).isEqualTo(tradeId);
            assertThat(result.getStatus()).isEqualTo("PENDING");
        }

        @Test
        @DisplayName("应该能从数据库查询到记录")
        void shouldBeAbleToQueryFromDatabase() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            StpGenerateTicket created = tradeService.createTrade(tradeId, tradeData);

            // When
            StpGenerateTicket queried = repository.findByEventId(created.getEventId());

            // Then
            assertThat(queried).isNotNull();
            assertThat(queried.getTradeData()).isEqualTo(tradeData);
            assertThat(queried.getCreatedAt()).isNotNull();
        }
    }

    @Nested
    @DisplayName("TC1-003: event_id 唯一性验证")
    class EventIdUniqueness {

        @Test
        @DisplayName("每次创建应该生成不同的 event_id")
        void shouldGenerateUniqueEventId() {
            // Given & When
            String tradeId1 = TestDataFactory.generateTradeId();
            String tradeId2 = TestDataFactory.generateTradeId();
            StpGenerateTicket ticket1 = tradeService.createTrade(tradeId1,
                    TestDataFactory.createEquitySwapTradeData(tradeId1));
            StpGenerateTicket ticket2 = tradeService.createTrade(tradeId2,
                    TestDataFactory.createEquitySwapTradeData(tradeId2));

            // Then
            assertThat(ticket1.getEventId()).isNotEqualTo(ticket2.getEventId());
        }
    }

    @Nested
    @DisplayName("TC1-004: 初始状态验证")
    class InitialStatusValidation {

        @Test
        @DisplayName("新建记录的状态应该是 PENDING")
        void shouldHavePendingStatus() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();

            // When
            StpGenerateTicket result = tradeService.createTrade(tradeId,
                    TestDataFactory.createEquitySwapTradeData(tradeId));

            // Then
            assertThat(result.getStatus()).isEqualTo("PENDING");
        }
    }
}
```

### 4.2 OTC Subscription Producer 测试

```java
package com.elite.test.component.architecture1;

import com.elite.kafka.producer.OtcSubscriptionProducer;
import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.KafkaTestHelper;
import com.elite.test.util.TestDataFactory;
import com.elite.test.container.TestContainersConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * OTC Subscription Producer 测试
 * 测试架构1的轮询发送阶段
 */
class OtcSubscriptionProducerTest extends AbstractEliteIntegrationTest {

    private static final String TOPIC = "stp_generate_ticket_events";

    @Autowired
    private OtcSubscriptionProducer producer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;
    private KafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
        kafkaHelper = new KafkaTestHelper(TestContainersConfig.getKafkaBootstrapServers());
    }

    @Nested
    @DisplayName("TC1-010: OTC Subscription 正常轮询")
    class NormalPolling {

        @Test
        @DisplayName("应该轮询 PENDING 状态的记录并发送到 Kafka")
        void shouldPollPendingRecordsAndSendToKafka() {
            // Given - 插入 PENDING 状态的记录
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "PENDING");

            // When - 触发轮询（或等待定时任务执行）
            producer.pollAndSend();

            // Then - 验证 Kafka 收到消息
            List<ConsumerRecord<String, String>> records = kafkaHelper.consumeMessages(
                    TOPIC, 1, Duration.ofSeconds(10));

            assertThat(records).hasSize(1);
            assertThat(records.get(0).key()).isEqualTo(tradeId);
            assertThat(records.get(0).value()).contains(eventId);
        }
    }

    @Nested
    @DisplayName("TC1-012: stp_generate_ticket_audit 记录生成")
    class AuditRecordGeneration {

        @Test
        @DisplayName("发送消息后应该生成审计记录")
        void shouldGenerateAuditRecordAfterSending() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "PENDING");

            // When
            producer.pollAndSend();

            // Then - 等待审计记录生成
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .until(() -> dbHelper.auditRecordExists(eventId));

            assertThat(dbHelper.getAuditKafkaOffset(eventId)).isNotNull();
        }
    }

    @Nested
    @DisplayName("TC1-015: 状态更新验证")
    class StatusUpdateValidation {

        @Test
        @DisplayName("发送成功后状态应该从 PENDING 变为 SENT")
        void shouldUpdateStatusFromPendingToSent() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "PENDING");

            // When
            producer.pollAndSend();

            // Then
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .until(() -> "SENT".equals(dbHelper.getStpGenerateTicketStatus(eventId)));
        }
    }

    @Nested
    @DisplayName("TC1-011: 轮询批量处理能力")
    class BatchProcessing {

        @Test
        @DisplayName("应该能批量处理多条 PENDING 记录")
        void shouldBatchProcessMultiplePendingRecords() {
            // Given - 插入多条记录
            int recordCount = 10;
            for (int i = 0; i < recordCount; i++) {
                String tradeId = TestDataFactory.generateTradeId();
                dbHelper.insertStpGenerateTicket(tradeId,
                        TestDataFactory.createEquitySwapTradeData(tradeId), "PENDING");
            }

            // When
            producer.pollAndSend();

            // Then - 验证所有消息都被发送
            List<ConsumerRecord<String, String>> records = kafkaHelper.consumeMessages(
                    TOPIC, recordCount, Duration.ofSeconds(30));

            assertThat(records).hasSize(recordCount);

            // 验证数据库状态
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> dbHelper.countPendingStpGenerateTickets() == 0);
        }
    }
}
```

### 4.3 Downstream Consumer 测试

```java
package com.elite.test.component.architecture1;

import com.elite.kafka.consumer.DownstreamConsumer;
import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.KafkaTestHelper;
import com.elite.test.util.TestDataFactory;
import com.elite.test.container.TestContainersConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Downstream Consumer 测试
 * 测试架构1的消费处理阶段
 */
class DownstreamConsumerTest extends AbstractEliteIntegrationTest {

    private static final String TOPIC = "stp_generate_ticket_events";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;
    private KafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
        kafkaHelper = new KafkaTestHelper(TestContainersConfig.getKafkaBootstrapServers());
    }

    @Nested
    @DisplayName("TC1-020: 正常消息消费")
    class NormalMessageConsumption {

        @Test
        @DisplayName("应该成功消费消息并处理")
        void shouldConsumeAndProcessMessage() {
            // Given - 插入原始记录并发送消息
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "SENT");

            String eventMessage = TestDataFactory.createStpGenerateTicketEvent(eventId, tradeId, tradeData);

            // When - 发送消息到 Kafka
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then - 等待消费者处理完成
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> dbHelper.ticketExists(eventId));
        }
    }

    @Nested
    @DisplayName("TC1-022: stp_ticket 记录生成")
    class TicketRecordGeneration {

        @Test
        @DisplayName("消费成功后应该生成 stp_ticket 记录")
        void shouldGenerateTicketRecord() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "SENT");

            String eventMessage = TestDataFactory.createStpGenerateTicketEvent(eventId, tradeId, tradeData);

            // When
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> dbHelper.ticketExists(eventId));

            assertThat(dbHelper.ticketExists(eventId)).isTrue();
        }
    }

    @Nested
    @DisplayName("TC1-024: 状态更新 SENT → COMPLETED")
    class StatusUpdateToCompleted {

        @Test
        @DisplayName("消费成功后应该将状态更新为 COMPLETED")
        void shouldUpdateStatusToCompleted() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "SENT");

            String eventMessage = TestDataFactory.createStpGenerateTicketEvent(eventId, tradeId, tradeData);

            // When
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> "COMPLETED".equals(dbHelper.getStpGenerateTicketStatus(eventId)));
        }
    }

    @Nested
    @DisplayName("TC1-026: 消费幂等性验证")
    class IdempotencyValidation {

        @Test
        @DisplayName("重复消费相同消息不应产生重复数据")
        void shouldNotCreateDuplicateDataOnDuplicateConsumption() {
            // Given - 先消费一次
            String tradeId = TestDataFactory.generateTradeId();
            String tradeData = TestDataFactory.createEquitySwapTradeData(tradeId);
            String eventId = dbHelper.insertStpGenerateTicket(tradeId, tradeData, "SENT");

            String eventMessage = TestDataFactory.createStpGenerateTicketEvent(eventId, tradeId, tradeData);

            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> "COMPLETED".equals(dbHelper.getStpGenerateTicketStatus(eventId)));

            // When - 再次发送相同消息
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then - 等待处理，验证没有产生重复数据
            Thread.sleep(5000); // 等待一段时间确保消息被处理

            // 状态应该仍然是 COMPLETED
            assertThat(dbHelper.getStpGenerateTicketStatus(eventId)).isEqualTo("COMPLETED");

            // ticket 记录应该只有一条
            assertThat(dbHelper.ticketExists(eventId)).isTrue();
        }
    }
}
```

---

## 5. 架构2测试实现：全异步 Payment 流程

### 5.1 Post Trade Action 测试

```java
package com.elite.test.component.architecture2;

import com.elite.entity.PostTradeAction;
import com.elite.repository.PostTradeActionRepository;
import com.elite.service.PaymentService;
import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Post Trade Action 测试
 * 测试架构2的事件发送阶段
 */
class PostTradeActionTest extends AbstractEliteIntegrationTest {

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PostTradeActionRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
    }

    @Nested
    @DisplayName("TC2-002: post_trade_action 表记录创建")
    class RecordCreation {

        @Test
        @DisplayName("触发 Payment 事件应该创建 post_trade_action 记录")
        void shouldCreatePostTradeActionRecord() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();

            // When
            PostTradeAction result = paymentService.triggerPayment(tradeId);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getActionId()).isNotBlank();
            assertThat(result.getTradeId()).isEqualTo(tradeId);
            assertThat(result.getActionType()).isEqualTo("PAYMENT");
            assertThat(result.getStatus()).isEqualTo("PENDING");
        }
    }

    @Nested
    @DisplayName("TC2-004: 初始状态 PENDING 验证")
    class InitialStatusValidation {

        @Test
        @DisplayName("新建记录的状态应该是 PENDING")
        void shouldHavePendingStatus() {
            // Given
            String tradeId = TestDataFactory.generateTradeId();

            // When
            PostTradeAction result = paymentService.triggerPayment(tradeId);

            // Then
            assertThat(result.getStatus()).isEqualTo("PENDING");
            assertThat(result.getPaymentId()).isNull();
            assertThat(result.getErrorMessage()).isNull();
        }
    }
}
```

### 5.2 Payment Consumer 测试

```java
package com.elite.test.component.architecture2;

import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.TestDataFactory;
import com.elite.test.container.TestContainersConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Payment Consumer 测试
 * 测试架构2的消费处理阶段
 */
class PaymentConsumerTest extends AbstractEliteIntegrationTest {

    private static final String TOPIC = "post_trade_action";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;
    private static WireMockServer wireMockServer;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(8089);
        wireMockServer.start();
        WireMock.configureFor("localhost", 8089);
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
        wireMockServer.resetAll();
    }

    @Nested
    @DisplayName("TC2-011: Payment API 调用成功")
    class PaymentApiSuccess {

        @Test
        @DisplayName("应该成功调用 Payment API 并更新状态")
        void shouldCallPaymentApiAndUpdateStatus() {
            // Given - 配置 Mock Payment API 返回成功
            String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8);
            wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody(TestDataFactory.createPaymentApiSuccessResponse(paymentId))));

            // 插入 post_trade_action 记录
            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");

            // 发送 Kafka 消息
            String eventMessage = TestDataFactory.createPostTradeActionEvent(actionId, tradeId, "PAYMENT");

            // When
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> "SUCCESS".equals(dbHelper.getPostTradeActionStatus(actionId)));

            assertThat(dbHelper.getPostTradeActionPaymentId(actionId)).isEqualTo(paymentId);

            // 验证 API 被调用
            wireMockServer.verify(postRequestedFor(urlEqualTo("/api/payment")));
        }
    }

    @Nested
    @DisplayName("TC2-014: Payment API 调用失败处理")
    class PaymentApiFailure {

        @Test
        @DisplayName("Payment API 返回 500 时应该标记为 FAILED")
        void shouldMarkAsFailedWhenApiReturns500() {
            // Given - 配置 Mock Payment API 返回错误
            wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                    .willReturn(aResponse()
                            .withStatus(500)
                            .withBody(TestDataFactory.createPaymentApiErrorResponse(
                                    "INTERNAL_ERROR", "Internal server error"))));

            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");
            String eventMessage = TestDataFactory.createPostTradeActionEvent(actionId, tradeId, "PAYMENT");

            // When
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then - 由于有重试机制，等待足够时间
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> {
                        String status = dbHelper.getPostTradeActionStatus(actionId);
                        return "FAILED".equals(status) || "RETRY_EXHAUSTED".equals(status);
                    });
        }

        @Test
        @DisplayName("Payment API 返回 400 时不应该重试")
        void shouldNotRetryWhenApiReturns400() {
            // Given
            wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                    .willReturn(aResponse()
                            .withStatus(400)
                            .withBody(TestDataFactory.createPaymentApiErrorResponse(
                                    "BAD_REQUEST", "Invalid request"))));

            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");
            String eventMessage = TestDataFactory.createPostTradeActionEvent(actionId, tradeId, "PAYMENT");

            // When
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> "FAILED".equals(dbHelper.getPostTradeActionStatus(actionId)));

            // 验证只调用了一次（无重试）
            wireMockServer.verify(1, postRequestedFor(urlEqualTo("/api/payment")));
        }
    }

    @Nested
    @DisplayName("TC2-018: 消费幂等性验证")
    class IdempotencyValidation {

        @Test
        @DisplayName("重复消费不应该重复调用 Payment API")
        void shouldNotCallPaymentApiTwiceForSameMessage() {
            // Given
            String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8);
            wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withBody(TestDataFactory.createPaymentApiSuccessResponse(paymentId))));

            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");
            String eventMessage = TestDataFactory.createPostTradeActionEvent(actionId, tradeId, "PAYMENT");

            // 第一次消费
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> "SUCCESS".equals(dbHelper.getPostTradeActionStatus(actionId)));

            // When - 再次发送相同消息
            kafkaTemplate.send(TOPIC, tradeId, eventMessage);

            // Then - 等待处理
            Thread.sleep(5000);

            // Payment API 应该只被调用一次
            wireMockServer.verify(1, postRequestedFor(urlEqualTo("/api/payment")));
        }
    }
}
```

### 5.3 WebSocket 推送测试

```java
package com.elite.test.component.architecture2;

import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.TestDataFactory;
import com.elite.test.util.WebSocketTestClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WebSocket 状态推送测试
 * 测试架构2的实时通知功能
 */
class WebSocketPushTest extends AbstractEliteIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;
    private WebSocketTestClient wsClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
        wsClient = new WebSocketTestClient("ws://localhost:" + port + "/ws/trade-status");
        wsClient.connectBlocking(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        if (wsClient != null) {
            wsClient.close();
        }
    }

    @Nested
    @DisplayName("TC2-020: 状态变更推送验证")
    class StatusChangePush {

        @Test
        @DisplayName("状态变更时应该通过 WebSocket 推送")
        void shouldPushStatusChangeViaWebSocket() throws Exception {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");

            // 订阅该交易的状态更新
            wsClient.subscribeToTrade(tradeId);

            // When - 更新状态
            dbHelper.updatePostTradeActionStatus(actionId, "SUCCESS", "PAY-12345");

            // Then - 等待收到 WebSocket 消息
            boolean received = wsClient.awaitMessages(1, 10, TimeUnit.SECONDS);
            assertThat(received).isTrue();

            List<String> messages = wsClient.getReceivedMessages();
            assertThat(messages).isNotEmpty();

            JsonNode message = objectMapper.readTree(messages.get(messages.size() - 1));
            assertThat(message.get("tradeId").asText()).isEqualTo(tradeId);
            assertThat(message.get("status").asText()).isEqualTo("SUCCESS");
            assertThat(message.get("paymentId").asText()).isEqualTo("PAY-12345");
        }
    }

    @Nested
    @DisplayName("TC2-022: SUCCESS 状态推送")
    class SuccessStatusPush {

        @Test
        @DisplayName("SUCCESS 状态推送应该包含 paymentId")
        void shouldIncludePaymentIdInSuccessPush() throws Exception {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");
            String paymentId = "PAY-" + System.currentTimeMillis();

            wsClient.subscribeToTrade(tradeId);

            // When
            dbHelper.updatePostTradeActionStatus(actionId, "SUCCESS", paymentId);

            // Then
            wsClient.awaitMessages(1, 10, TimeUnit.SECONDS);

            List<String> messages = wsClient.getReceivedMessages();
            JsonNode message = objectMapper.readTree(messages.get(messages.size() - 1));

            assertThat(message.has("paymentId")).isTrue();
            assertThat(message.get("paymentId").asText()).isEqualTo(paymentId);
        }
    }

    @Nested
    @DisplayName("TC2-023: FAILED 状态推送")
    class FailedStatusPush {

        @Test
        @DisplayName("FAILED 状态推送应该包含错误信息")
        void shouldIncludeErrorMessageInFailedPush() throws Exception {
            // Given
            String tradeId = TestDataFactory.generateTradeId();
            String actionId = dbHelper.insertPostTradeAction(tradeId, "PAYMENT");

            wsClient.subscribeToTrade(tradeId);

            // When - 通过 SQL 直接更新（模拟失败场景）
            jdbcTemplate.update(
                    "UPDATE post_trade_action SET status = 'FAILED', " +
                    "error_message = 'Payment API timeout' WHERE action_id = ?",
                    actionId);

            // Then
            wsClient.awaitMessages(1, 10, TimeUnit.SECONDS);

            List<String> messages = wsClient.getReceivedMessages();
            JsonNode message = objectMapper.readTree(messages.get(messages.size() - 1));

            assertThat(message.get("status").asText()).isEqualTo("FAILED");
            assertThat(message.has("errorMessage")).isTrue();
            assertThat(message.get("errorMessage").asText()).contains("timeout");
        }
    }
}
```

---

## 6. 端到端测试

### 6.1 完整流程 E2E 测试

```java
package com.elite.test.e2e;

import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import com.elite.test.util.TestDataFactory;
import com.elite.test.util.WebSocketTestClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Payment 流程端到端测试
 * 测试完整的架构2流程：前端触发 -> Kafka -> Consumer -> Payment API -> 回写 -> WebSocket
 */
class PaymentFlowE2ETest extends AbstractEliteIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;
    private WebSocketTestClient wsClient;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static WireMockServer wireMockServer;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(8089);
        wireMockServer.start();
        WireMock.configureFor("localhost", 8089);
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
        wsClient = new WebSocketTestClient("ws://localhost:" + port + "/ws/trade-status");
        wsClient.connectBlocking(5, TimeUnit.SECONDS);
        wireMockServer.resetAll();
    }

    @AfterEach
    void tearDown() {
        if (wsClient != null) {
            wsClient.close();
        }
    }

    @Test
    @DisplayName("TC2-040: 完整 Payment 流程 - 成功场景")
    void shouldCompleteFullPaymentFlowSuccessfully() throws Exception {
        // ============ Step 1: 配置 Mock Payment API ============
        String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8);
        wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(TestDataFactory.createPaymentApiSuccessResponse(paymentId))));

        // ============ Step 2: 创建交易（前置条件）============
        String tradeId = TestDataFactory.generateTradeId();

        // ============ Step 3: 订阅 WebSocket ============
        wsClient.subscribeToTrade(tradeId);

        // ============ Step 4: 触发 Payment 请求 ============
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/trades/" + tradeId + "/payment",
                new HttpEntity<>("{}", headers),
                String.class
        );

        // 验证 API 响应
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // ============ Step 5: 等待并验证 WebSocket 消息 ============
        // 应该收到两条消息：PENDING 和 SUCCESS
        boolean received = wsClient.awaitMessages(2, 30, TimeUnit.SECONDS);
        assertThat(received).isTrue();

        List<String> messages = wsClient.getReceivedMessages();
        assertThat(messages).hasSizeGreaterThanOrEqualTo(2);

        // 验证第一条消息（PENDING）
        JsonNode pendingMsg = objectMapper.readTree(messages.get(0));
        assertThat(pendingMsg.get("tradeId").asText()).isEqualTo(tradeId);
        assertThat(pendingMsg.get("status").asText()).isEqualTo("PENDING");

        // 验证最后一条消息（SUCCESS）
        JsonNode successMsg = objectMapper.readTree(messages.get(messages.size() - 1));
        assertThat(successMsg.get("tradeId").asText()).isEqualTo(tradeId);
        assertThat(successMsg.get("status").asText()).isEqualTo("SUCCESS");
        assertThat(successMsg.get("paymentId").asText()).isEqualTo(paymentId);

        // ============ Step 6: 验证数据库状态 ============
        // 查询 post_trade_action 表
        List<java.util.Map<String, Object>> records = jdbcTemplate.queryForList(
                "SELECT * FROM post_trade_action WHERE trade_id = ?", tradeId);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("status")).isEqualTo("SUCCESS");
        assertThat(records.get(0).get("payment_id")).isEqualTo(paymentId);

        // ============ Step 7: 验证 Payment API 被调用 ============
        wireMockServer.verify(postRequestedFor(urlEqualTo("/api/payment")));
    }

    @Test
    @DisplayName("TC2-041: 完整 Payment 流程 - 失败后重试成功")
    void shouldRetryAndSucceedAfterInitialFailure() throws Exception {
        // ============ 配置 Mock: 前两次失败，第三次成功 ============
        String paymentId = "PAY-RETRY-" + UUID.randomUUID().toString().substring(0, 8);

        wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("First Failure"));

        wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("First Failure")
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("Second Failure"));

        wireMockServer.stubFor(post(urlEqualTo("/api/payment"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("Second Failure")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(TestDataFactory.createPaymentApiSuccessResponse(paymentId))));

        // ============ 触发 Payment ============
        String tradeId = TestDataFactory.generateTradeId();
        wsClient.subscribeToTrade(tradeId);

        restTemplate.postForEntity(
                "/api/trades/" + tradeId + "/payment",
                new HttpEntity<>("{}", new HttpHeaders()),
                String.class
        );

        // ============ 等待最终成功 ============
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> {
                    List<java.util.Map<String, Object>> records = jdbcTemplate.queryForList(
                            "SELECT status FROM post_trade_action WHERE trade_id = ?", tradeId);
                    return !records.isEmpty() && "SUCCESS".equals(records.get(0).get("status"));
                });

        // ============ 验证重试次数 ============
        wireMockServer.verify(3, postRequestedFor(urlEqualTo("/api/payment")));
    }
}
```

---

## 7. 数据一致性验证测试

```java
package com.elite.test.integration;

import com.elite.test.base.AbstractEliteIntegrationTest;
import com.elite.test.util.DatabaseTestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据一致性验证测试
 * 验证两种架构的数据一致性
 */
class DataConsistencyTest extends AbstractEliteIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseTestHelper dbHelper;

    @BeforeEach
    void setUp() {
        dbHelper = new DatabaseTestHelper(jdbcTemplate);
    }

    @Test
    @DisplayName("架构1: SENT 状态的记录必须有对应的审计记录")
    void architecture1SentRecordsShouldHaveAuditRecords() {
        // 查询 SENT 但无审计记录的数据
        List<Map<String, Object>> inconsistentRecords = jdbcTemplate.queryForList(
                "SELECT t.event_id, t.trade_id, t.status " +
                "FROM stp_generate_ticket t " +
                "LEFT JOIN stp_generate_ticket_audit a ON t.event_id = a.event_id " +
                "WHERE t.status = 'SENT' AND a.event_id IS NULL"
        );

        assertThat(inconsistentRecords)
                .as("所有 SENT 状态的记录都应该有审计记录")
                .isEmpty();
    }

    @Test
    @DisplayName("架构1: COMPLETED 状态的记录必须有对应的 ticket")
    void architecture1CompletedRecordsShouldHaveTickets() {
        List<Map<String, Object>> inconsistentRecords = jdbcTemplate.queryForList(
                "SELECT t.event_id, t.trade_id, t.status " +
                "FROM stp_generate_ticket t " +
                "LEFT JOIN stp_ticket k ON t.event_id = k.event_id " +
                "WHERE t.status = 'COMPLETED' AND k.ticket_id IS NULL"
        );

        assertThat(inconsistentRecords)
                .as("所有 COMPLETED 状态的记录都应该有 ticket")
                .isEmpty();
    }

    @Test
    @DisplayName("架构2: SUCCESS 状态的记录必须有 payment_id")
    void architecture2SuccessRecordsShouldHavePaymentId() {
        List<Map<String, Object>> inconsistentRecords = dbHelper.checkArchitecture2Consistency();

        assertThat(inconsistentRecords)
                .as("所有 SUCCESS 状态的记录都应该有 payment_id")
                .isEmpty();
    }

    @Test
    @DisplayName("架构2: FAILED 状态的记录必须有错误信息")
    void architecture2FailedRecordsShouldHaveErrorMessage() {
        List<Map<String, Object>> inconsistentRecords = jdbcTemplate.queryForList(
                "SELECT * FROM post_trade_action " +
                "WHERE status IN ('FAILED', 'RETRY_EXHAUSTED') " +
                "AND error_message IS NULL"
        );

        assertThat(inconsistentRecords)
                .as("所有 FAILED 状态的记录都应该有错误信息")
                .isEmpty();
    }

    @Test
    @DisplayName("检测超时未处理的 PENDING 记录")
    void shouldDetectStaleRecords() {
        // 查询超过 5 分钟仍为 PENDING 的记录
        List<Map<String, Object>> staleRecords = jdbcTemplate.queryForList(
                "SELECT * FROM post_trade_action " +
                "WHERE status = 'PENDING' " +
                "AND created_at < NOW() - INTERVAL 5 MINUTE"
        );

        // 这个测试可能会失败，表示有异常情况需要处理
        if (!staleRecords.isEmpty()) {
            System.err.println("警告: 发现 " + staleRecords.size() + " 条超时未处理的记录");
            staleRecords.forEach(r -> System.err.println("  - " + r));
        }

        // 在生产环境中，这应该触发告警
        assertThat(staleRecords)
                .as("不应该有超过 5 分钟仍为 PENDING 的记录")
                .isEmpty();
    }
}
```

---

## 8. 配置文件

### 8.1 测试配置 (application-test.yml)

```yaml
spring:
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2

  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: elite-test-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      properties:
        spring.json.trusted.packages: "*"
    producer:
      acks: all
      retries: 3
    listener:
      ack-mode: record
      concurrency: 3

  jpa:
    hibernate:
      ddl-auto: none
    show-sql: true

# Elite 特定配置
elite:
  kafka:
    topics:
      stp-generate-ticket: stp_generate_ticket_events
      post-trade-action: post_trade_action
  payment:
    api:
      url: http://localhost:8089/api/payment
      timeout: 5000
      retry:
        max-attempts: 3
        backoff-delay: 1000
  polling:
    interval: 1000
    batch-size: 100

# 日志配置
logging:
  level:
    org.testcontainers: INFO
    com.github.dockerjava: WARN
    org.apache.kafka: WARN
    com.elite: DEBUG
    org.springframework.kafka: DEBUG
```

### 8.2 数据库初始化脚本 (schema.sql)

```sql
-- stp_generate_ticket 表
CREATE TABLE IF NOT EXISTS stp_generate_ticket (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    trade_id VARCHAR(32) NOT NULL,
    trade_data JSON NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_trade_id (trade_id)
);

-- stp_generate_ticket_audit 表
CREATE TABLE IF NOT EXISTS stp_generate_ticket_audit (
    audit_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    kafka_topic VARCHAR(128),
    kafka_partition INT,
    kafka_offset BIGINT,
    message_key VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'SENT',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES stp_generate_ticket(event_id),
    INDEX idx_event_id (event_id)
);

-- stp_ticket 表
CREATE TABLE IF NOT EXISTS stp_ticket (
    ticket_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    ticket_data JSON,
    downstream_status VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES stp_generate_ticket(event_id),
    INDEX idx_event_id (event_id)
);

-- post_trade_action 表（对账台）
CREATE TABLE IF NOT EXISTS post_trade_action (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    action_id VARCHAR(64) NOT NULL UNIQUE,
    trade_id VARCHAR(32) NOT NULL,
    action_type VARCHAR(32) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    payment_id VARCHAR(64),
    retry_count INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_trade_id (trade_id),
    INDEX idx_status (status),
    INDEX idx_action_type (action_type)
);
```

---

## 9. 下一步

1. 根据实际代码结构调整测试代码
2. 补充更多边界场景测试
3. 集成到 CI/CD 流水线
4. 建立测试覆盖率监控

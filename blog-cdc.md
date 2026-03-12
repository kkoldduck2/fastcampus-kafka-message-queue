# Spring Boot + Kafka로 CDC 구현하기

## CDC란?
DB에 데이터가 변경되면, 그 변경 사실을 다른 시스템에 자동으로 알려주는 패턴.

---

## 목표
서비스 코드에 Kafka 발행 로직을 직접 쓰지 않고, DB 변경만 하면 자동으로 Kafka에 이벤트가 나가게 만들고 싶다.

```
// 이렇게 하고 싶지 않다 (서비스에 Kafka 로직이 섞임)
public void save(MyModel model) {
    myJpaRepository.save(entity);
    kafkaTemplate.send(...);  // 비즈니스 로직과 메시징이 섞임
}

// 이렇게 하고 싶다 (서비스는 DB만 신경씀)
public void save(MyModel model) {
    myJpaRepository.save(entity);  // 이것만 하면 Kafka 발행은 알아서
}
```

---

## 방법 1: JPA EntityListener

JPA에는 엔티티(DB 객체)에 변경이 생기면 자동으로 호출되는 리스너 기능이 있다.
이걸 활용해서 **DB 변경 → Kafka 발행**을 자동화했다.

### 흐름

```
Service: repository.save(entity) 호출
  ↓
JPA: DB에 INSERT 실행
  ↓
JPA: "EntityListener 등록돼 있네?" → handleCreate() 자동 호출
  ↓
EntityListener: Kafka에 CDC 메시지 발행
```

### 엔티티에 리스너 등록

```java
@EntityListeners(value = MyEntityListener.class)  // 이 한 줄이 핵심
@Entity(name = "my_table")
public class MyEntity {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String userName;
    private String content;
    // ...
}
```

### 리스너 구현

```java
public class MyEntityListener {

    private MyCdcProducer myCdcProducer;

    @PostPersist   // INSERT 후 자동 호출
    public void handleCreate(MyEntity myEntity) {
        myCdcProducer.sendMessage(
            toMessage(myEntity.getId(), myEntity, OperationType.CREATE)
        );
    }

    @PostUpdate    // UPDATE 후 자동 호출
    public void handleUpdate(MyEntity myEntity) {
        myCdcProducer.sendMessage(
            toMessage(myEntity.getId(), myEntity, OperationType.UPDATE)
        );
    }

    @PostRemove    // DELETE 후 자동 호출
    public void handleDelete(MyEntity myEntity) {
        myCdcProducer.sendMessage(
            toMessage(myEntity.getId(), null, OperationType.DELETE)
        );
    }
}
```

| 어노테이션 | 발동 시점 | OperationType | payload |
|-----------|----------|---------------|---------|
| @PostPersist | INSERT 후 | CREATE | 전체 데이터 |
| @PostUpdate | UPDATE 후 | UPDATE | 전체 데이터 |
| @PostRemove | DELETE 후 | DELETE | null (id만) |

### Kafka에 실제로 날아가는 메시지

```json
{ "id": 1, "operationType": "CREATE", "payload": { "userName": "홍길동", "content": "안녕" } }
{ "id": 1, "operationType": "UPDATE", "payload": { "userName": "홍길동", "content": "수정됨" } }
{ "id": 1, "operationType": "DELETE", "payload": null }
```

### 장점
- 서비스 코드가 깔끔하다. DB 저장만 하면 끝.
- CRUD 어디서든 동일하게 동작한다.

### 문제점
EntityListener는 **JPA 트랜잭션 안에서** 실행된다.
즉, Kafka 발행이 실패하면 DB 저장까지 롤백될 수 있다.
반대로, Kafka 발행은 됐는데 이후 트랜잭션이 롤백되면 DB에는 없는 데이터가 Kafka에 나간 셈이 된다.

```
DB 저장 ✅ → Kafka 발행 ❌ → 트랜잭션 롤백 (DB도 취소됨)
DB 저장 ✅ → Kafka 발행 ✅ → 이후 로직 실패 → 트랜잭션 롤백 (Kafka는 이미 나감)
```

---

## 방법 2: TransactionalEventListener (대안)

위 문제를 줄이기 위한 접근. **DB 트랜잭션이 확실히 커밋된 후에** Kafka를 발행한다.

```java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
@Async
public void transactionalEventListenerAfterCommit(MyCdcApplicationEvent event) {
    myCdcProducer.sendMessage(
        toMessage(event.getId(), event.getMyModel(), event.getOperationType())
    );
}
```

### EntityListener와 비교

| | EntityListener | TransactionalEventListener |
|---|---|---|
| 발행 시점 | 트랜잭션 안 (커밋 전) | 트랜잭션 커밋 후 |
| DB 롤백 시 | Kafka도 안 나감 | 애초에 발행 안 함 |
| Kafka 실패 시 | DB까지 롤백될 수 있음 | DB는 이미 커밋됨 (안전) |
| 새로운 문제 | - | DB는 커밋됐는데 Kafka 발행이 실패하면? |

### 남아있는 문제
DB 커밋 후 Kafka 발행이 실패하면, DB에는 있는데 다른 서비스는 모르는 상황이 생긴다.
이건 애플리케이션 레벨 CDC의 근본적인 한계다.

---

## 정합성 문제를 완전히 해결하려면?

두 방식 모두 **DB와 Kafka가 별개 시스템**이라서 100% 정합성을 보장하기 어렵다.

완전한 해결이 필요하면:

- **Transactional Outbox 패턴**: 메시지를 별도 DB 테이블에 먼저 저장 → 별도 프로세스가 읽어서 Kafka로 발행
- **로그 기반 CDC (Debezium 등)**: DB의 변경 로그(binlog)를 직접 읽어서 Kafka로 발행. 애플리케이션 코드 수정 불필요.

---

## 정리

| 방식 | 구현 난이도 | 정합성 | 특징 |
|------|-----------|--------|------|
| EntityListener | 쉬움 | 불완전 | 트랜잭션 안에서 발행, 실패 시 롤백 |
| TransactionalEventListener | 쉬움 | 불완전 | 커밋 후 발행, Kafka 실패 시 유실 가능 |
| Transactional Outbox | 보통 | 보장 | DB 테이블 + 별도 프로세스 필요 |
| 로그 기반 CDC (Debezium) | 높음 | 보장 | 별도 인프라 필요, 가장 안정적 |

프로젝트 규모와 정합성 요구 수준에 따라 선택하면 된다.
간단한 서비스라면 EntityListener로 충분하고, 돈이 오가는 서비스라면 Outbox나 로그 기반 CDC를 고려해야 한다.

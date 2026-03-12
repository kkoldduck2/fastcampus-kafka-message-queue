# Chapter 5 - CDC (Change Data Capture) 정리

## CDC란?
DB에 데이터가 변경되면, 그 변경 사실을 Kafka로 자동 전파하는 패턴.
서비스 간 직접 호출 없이, 각자 Kafka 메시지를 소비하면 되므로 서비스끼리 독립적으로 동작할 수 있다.

---

## 전체 흐름

```
클라이언트 (POST /greetings)
  │
  ▼
MyController.create()
  │  사용자 입력 → MyModel.create()로 객체 생성
  ▼
MyServiceImpl.save()
  │  MyModel → MyEntity로 변환
  │  myJpaRepository.save() → DB에 INSERT
  ▼
JPA가 자동으로 MyEntityListener 호출 (@PostPersist)
  │  MyEntity → MyCdcMessage로 변환
  │  OperationType 지정 (CREATE / UPDATE / DELETE)
  ▼
MyCdcProducer.sendMessage()
  │  JSON 직렬화 → "my-cdc-topic"에 발행
  ▼
Kafka 브로커
  ▼
MyCdcConsumer.listen()
  │  JSON 역직렬화 → 메시지 처리
  │  acknowledgment.acknowledge() → 수동 오프셋 커밋
```

---

## 주요 클래스 역할

| 클래스 | 역할 |
|--------|------|
| MyController | REST API 진입점 (CRUD 엔드포인트) |
| MyServiceImpl | 비즈니스 로직, JpaRepository 호출 |
| MyEntity | DB 테이블(`my_table`)과 매핑되는 객체 |
| MyModel | 서비스 내부에서 쓰는 일반 객체 (DB 의존 없음) |
| MyModelConverter | MyEntity ↔ MyModel ↔ MyCdcMessage 변환 |
| MyEntityListener | JPA 엔티티 변경 감지 → Kafka 발행 트리거 |
| MyCdcProducer | Kafka에 CDC 메시지 발행 |
| MyCdcConsumer | Kafka에서 CDC 메시지 수신 및 처리 |

---

## 핵심 개념

### EntityListener - DB 변경 감지의 핵심
`@EntityListeners(MyEntityListener.class)`를 MyEntity에 붙이면,
JPA가 DB 작업 후 자동으로 리스너 메서드를 호출한다.

| JPA 이벤트 | 발동 시점 | OperationType | payload |
|------------|----------|---------------|---------|
| @PostPersist | INSERT 후 | CREATE | 전체 데이터 |
| @PostUpdate | UPDATE 후 | UPDATE | 전체 데이터 |
| @PostRemove | DELETE 후 | DELETE | null (id만) |

### OperationType (enum)
변경 종류를 타입 안전하게 표현: `CREATE`, `UPDATE`, `DELETE`

### MyCdcMessage - Kafka로 보내는 메시지 형태
```json
{
  "id": 1,
  "payload": { "userId": 42, "userName": "홍길동", ... },
  "operationType": "CREATE"
}
```

### MyEntity vs MyModel - 왜 분리?
- MyEntity: DB 전용 (JPA 어노테이션 포함)
- MyModel: 비즈니스 로직 전용 (순수 자바 객체)
- DB 구조가 바뀌어도 서비스 코드에 영향 없도록 분리

### 수동 오프셋 커밋
`acknowledgment.acknowledge()`를 직접 호출해야 "이 메시지 처리 완료" 처리됨.
안 하면 Kafka가 같은 메시지를 다시 보냄.

---

## Topic 상수
| 상수 | 토픽 이름 | 용도 |
|------|----------|------|
| MY_JSON_TOPIC | my-json-topic | 일반 메시지 |
| MY_CDC_TOPIC | my-cdc-topic | CDC 메시지 |

---

## 미사용 코드 (대안 방식)
`MyCdcApplicationEventListener` - `@TransactionalEventListener`를 사용해서
DB 커밋이 확실히 끝난 후에 Kafka 발행하는 방식. 현재는 EntityListener 방식을 사용 중.

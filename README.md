# message-broker

**Запуск**
1. Поднять PostgreSQL:

```bash
docker compose up -d postgres
```

2. Подготовить env:

```bash
cp .env.example .env
```

3. Установить `goose`, если его ещё нет:

```bash
go install github.com/pressly/goose/v3/cmd/goose@latest
```

4. Применить миграции:

```bash
goose -dir ./migrations postgres "host=localhost port=5432 user=message_broker password=message_broker dbname=message_broker sslmode=disable" up
```

5. Запустить сервис.

Для `bash`:

```bash
set -a
source .env
set +a
go run ./cmd/broker
```
По умолчанию сервис поднимается на `localhost:50051`.

**Основные переменные**

Обязательная:
- `ROOT_TOKEN`

Необязательные:
- `GRPC_ADDR`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `MAX_MESSAGE_SIZE`
- `MAX_ROUTING_KEY_LENGTH`
- `MAX_QUEUE_FILTERS`
- `MAX_IN_FLIGHT`

Полный пример есть в [.env.example](./.env.example).

**Базовые grpcurl примеры**
Все вызовы требуют header:

```text
x-root-token: <ROOT_TOKEN>
```

Создать `Exchange`:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"name":"corp"}' \
  localhost:50051 broker.BrokerService/CreateExchange
```

Создать `Queue`:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","durable":true,"auto_delete":false,"filters":["corp.users.*"],"max_attempts":3}' \
  localhost:50051 broker.BrokerService/RegisterQueue
```

Опубликовать сообщение:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","routing_key":"corp.users.create","payload":"aGVsbG8="}' \
  localhost:50051 broker.BrokerService/Publish
```

Добавить consumer:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","consumer_id":"consumer-1"}' \
  localhost:50051 broker.BrokerService/AddConsumer
```

Получить одно сообщение через fetch:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","consumer_id":"consumer-1","timeout_ms":5000}' \
  localhost:50051 broker.BrokerService/Fetch
```

Подтвердить сообщение:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","delivery_id":"<DELIVERY_ID>","consumer_id":"consumer-1"}' \
  localhost:50051 broker.BrokerService/Ack
```

Отклонить сообщение:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","delivery_id":"<DELIVERY_ID>","consumer_id":"consumer-1"}' \
  localhost:50051 broker.BrokerService/Nack
```

Получать сообщения через stream:

```bash
grpcurl -plaintext -H 'x-root-token: message_broker_root' \
  -d '{"exchange_name":"corp","queue_name":"users","consumer_id":"consumer-1"}' \
  localhost:50051 broker.BrokerService/StreamFetch
```

**Базовый пример Go-клиента**

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/SitnikovArtem06/message-broker/pkg/brokerclient"
)

func main() {
	ctx := context.Background()

	client, err := brokerclient.New(
		"localhost:50051",
		brokerclient.WithRootToken("message_broker_root"),
		brokerclient.WithConsumerID("consumer-1"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.CreateExchange(ctx, "corp"); err != nil {
		log.Fatal(err)
	}

	if err := client.RegisterQueue(
		ctx,
		"corp",
		"users",
		true,
		false,
		brokerclient.WithMaxAttempts(3),
		brokerclient.WithRoutingKey("corp.users.*"),
	); err != nil {
		log.Fatal(err)
	}

	if err := client.Publish(ctx, "corp", "corp.users.create", []byte("hello")); err != nil {
		log.Fatal(err)
	}

	msg, err := client.Fetch(ctx, "corp", "users", 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("routing_key=%s payload=%s attempts=%d", msg.RoutingKey(), string(msg.Payload()), msg.Attempts())

	if err := msg.Ack(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Пример stream:

```go
stream, err := client.StreamFetch(ctx, "corp", "users")
if err != nil {
	log.Fatal(err)
}

for {
	msg, err := stream.Receive()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("payload=%s", string(msg.Payload()))

	if err := msg.Ack(ctx); err != nil {
		log.Fatal(err)
	}
}
```

**Особенности**
- `RoutingKey` чувствителен к регистру
- `*` в фильтре означает ровно один токен
- `durable=true` и `auto_delete=true` одновременно запрещены
- для `Fetch` и `StreamFetch` сообщения нужно подтверждать через `Ack` или `NAck`
- после рестарта восстанавливаются durable-очереди и неподтверждённые durable-сообщения

**Тесты**

```bash
go test ./...
```

# Kafka Streams Revenue Alert System

Приложение для мониторинга выручки от продаж продуктов и генерации алертов при превышении порога.

## Структура проекта

- `RevenueAlertApp.java` - основное Kafka Streams приложение
- `DataGenerator.java` - генератор тестовых данных
- `AlertConsumer.java` - потребитель алертов
- `models/` - классы данных
- `serde/` - сериализаторы/десериализаторы

## Запуск

1. Запустите Kafka кластер
2. Создайте топики: purchases, products, revenue-alerts
3. Запустите DataGenerator
4. Запустите RevenueAlertApp
5. Запустите AlertConsumer для просмотра алертов

# Kafka Streams Revenue Alert System

Приложение для мониторинга выручки от продаж продуктов и генерации алертов при превышении порога.

## Описание

Система отслеживает суммарную выручку по каждому продукту за скользящее окно в 1 минуту и генерирует алерты, когда выручка превышает 3000.

## Архитектура

- **DataGenerator** - генерирует тестовые данные о продуктах и покупках
- **RevenueAlertApp** - Kafka Streams приложение для обработки данных и генерации алертов
- **AlertConsumer** - потребитель алертов для отображения уведомлений

## Технологии

- Java 11
- Kafka Streams 3.4.0
- Apache Kafka
- Maven
- Docker & Docker Compose

## Запуск

### 1. Запуск инфраструктуры
```bash
docker-compose up -d
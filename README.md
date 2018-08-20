### Information
This is Kafka Streams project for saving and processing quotes

### Cloning a project
git clone https://github.com/Efficeon/test-stock-streams.git

### Producer Kafka Project
git clone https://github.com/Efficeon/test-stock-producer.git

### Creating Topics
After runing the application, the topics will be created automatically.

Next topics will be create:

* "stock-topic-recent-data"
* "stock-topic-less"
* "stock-topic-more"
* "stock-topic-averages"


### Tasks (only part 2)
Есть входные данные, гипотетические котивки акций, нужно сгенерить поток котировок транформировать его и сохранить в кафку.

1. Нужно писать генератор которые будет продюсить в кафку котировки
ключь - String, вида "GOOG", "APL", "AMZN", любая небольшая строка, 5-6 разных "компаний"
значение - Double, это прайс который будет генерироваться в диапазоне, например, 1.5-3.5
Топик с котировками будет входом для Kafka Streams.
Нужно нагенерить 100-200 записей в этот топик что бы там были данные вида:
...
"APL" 1.8
"GOOG" 2.8
"APL" 3.2
"APL" 1.95
"GOOG" 3.0
"APL" 2.3
...
2. Написать на Kafka Streams программу которая будет:
  * хранить последний срез котировок в KTable.
  * хранить среднее значение котировок в KTable, например, по приведенным выше данным для "GOOG" это будет 2.9.
  * таблицы (1) и (2) также нужно дополнительно сливать в два топика в кафку.
  * разбить входной поток котировок на два других и сохранить их в два разных топика, один где значение котировок >= 2.5, в другом < 2.5.

+ на стримы тесты.
Можете обратить внимание на примеры где юзают встроенный однонодовый кластер.
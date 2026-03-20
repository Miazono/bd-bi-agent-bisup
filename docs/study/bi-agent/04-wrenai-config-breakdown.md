# Разбор `infra/wrenai/config.yaml`

## Зачем этот документ

Этот документ разбирает `infra/wrenai/config.yaml` по порядку, в том же порядке, в котором блоки расположены в самом файле.
Задача не просто перечислить параметры, а понять, как в проекте собран BI-контур WrenAI: какая модель отвечает за генерацию, где строятся эмбеддинги, какой движок исполняет SQL, где хранится векторный контекст и как через pipeline проходит запрос пользователя.

## Общая структура файла

Файл состоит из шести логических частей:

1. `type: llm`
2. `type: embedder`
3. два блока `type: engine`
4. `type: document_store`
5. `type: pipeline`
6. `settings`

Каждая часть отвечает за отдельный слой BI-системы, и именно вместе они образуют рабочую конфигурацию WrenAI.

## Блок `type: llm`

### Поля и значения

- `type: llm`
- `provider: litellm_llm`
- `timeout: 120`
- `models`
  - `alias: default`
  - `model: gpt-4o`
  - `context_window_size: 128000`
  - `kwargs.max_tokens: 4096`
  - `kwargs.n: 1`
  - `kwargs.seed: 0`
  - `kwargs.temperature: 0`

### Что означает каждый параметр

`type: llm` говорит системе, что дальше описывается блок языковой модели.

`provider: litellm_llm` означает, что WrenAI использует провайдер через `litellm`, то есть модель подключается не напрямую, а через абстракцию поставщика.

`timeout: 120` задаёт предельное время ожидания ответа от этого блока.

Внутри `models` описан один вариант модели:

- `alias: default` - имя, по которому модель будет ссылаться в других местах конфигурации;
- `model: gpt-4o` - фактическая модель;
- `context_window_size: 128000` - размер доступного контекста;
- `kwargs.max_tokens: 4096` - верхняя граница длины ответа;
- `kwargs.n: 1` - одна генерация;
- `kwargs.seed: 0` - фиксированный seed для более воспроизводимого поведения;
- `kwargs.temperature: 0` - нулевая температура, то есть минимизация случайности.

### Практический смысл

Для BI-агента это важный блок, потому что здесь задаётся модель, которая формирует SQL, объяснения и текстовые ответы.
Нулевая температура и фиксированный seed в текущем проекте указывают на стремление к предсказуемости: для NL2SQL важнее стабильный результат, чем творческая вариативность.

## Блок `type: embedder`

### Поля и значения

- `type: embedder`
- `provider: litellm_embedder`
- `models`
  - `alias: default`
  - `model: text-embedding-3-small`
  - `timeout: 120`

### Что означает каждый параметр

`type: embedder` объявляет блок модели эмбеддингов.

`provider: litellm_embedder` показывает, что эмбеддер тоже подключён через `litellm`.

`alias: default` снова задаёт стандартное имя, которым модель будет использоваться в pipeline.

`model: text-embedding-3-small` - сама модель эмбеддингов.

`timeout: 120` задаёт время ожидания для операций построения эмбеддингов.

### Практический смысл

Эмбеддер нужен для retrieval-слоя: он переводит схемы, инструкции, SQL-пары и другие текстовые объекты в векторное представление.
Именно на этом уровне происходит поиск похожих документов и похожих вопросов, который затем помогает LLM выбрать правильный контекст.

## Два блока `type: engine`

### Первый движок: `wren_ui`

Поля:

- `type: engine`
- `provider: wren_ui`
- `endpoint: http://wren-ui:3000`

Этот движок ссылается на сервис `wren-ui`.
По конфигу видно, что WrenAI использует его как часть основного внутреннего контура выполнения.

### Второй движок: `wren_ibis`

Поля:

- `type: engine`
- `provider: wren_ibis`
- `endpoint: http://ibis-server:8000`

Этот движок связан с `ibis-server`.
Он нужен для функциональности, которая опирается на Ibis и на SQL-функции, доступные через этот уровень исполнения.

### Что это означает в архитектуре

В конфиге видно два разных слоя исполнения:

- `wren_ui` - основной BI-сервисный движок;
- `wren_ibis` - специализированный движок для Ibis-части.

Такой разнос полезен, потому что не все операции в BI-контуре одинаковы по назначению: часть отвечает за генерацию и выполнение SQL, а часть - за вспомогательные SQL/Ibis-возможности.

## Блок `type: document_store`

### Поля и значения

- `type: document_store`
- `provider: qdrant`
- `location: http://qdrant:6333`
- `embedding_model_dim: 1536`
- `timeout: 120`
- `recreate_index: true`

### Что означает каждый параметр

`provider: qdrant` задаёт векторное хранилище.

`location: http://qdrant:6333` показывает адрес сервиса в контейнерном контуре.

`embedding_model_dim: 1536` должен совпадать с размерностью эмбеддингов, которые сюда кладутся.

`timeout: 120` задаёт время ожидания операций с хранилищем.

`recreate_index: true` означает, что индекс может пересоздаваться при старте или инициализации контура.

### Практический смысл

Qdrant хранит векторный контекст для retrieval.
Без этого BI-агенту было бы намного сложнее поднимать релевантные таблицы, инструкции, похожие вопросы и SQL-пары.

## Блок `type: pipeline`

### Что это за блок

Это самая объёмная часть конфигурации.
Здесь перечислены все pipe WrenAI, и для каждого из них указано, какие зависимости он использует:

- `llm`
- `embedder`
- `engine`
- `document_store`

То есть pipeline в этом файле - не абстрактный список шагов, а реальная карта того, как WrenAI связывает генерацию, retrieval, выполнение и вспомогательные этапы.

### Группа 1. Индексирование

К этой группе относятся:

- `db_schema_indexing`
- `historical_question_indexing`
- `table_description_indexing`
- `sql_pairs_indexing`
- `instructions_indexing`
- `project_meta_indexing`

Их роль - подготовить документы для последующего поиска.

Что видно по зависимости:

- `db_schema_indexing` использует `embedder` и `document_store`;
- `historical_question_indexing` использует `embedder` и `document_store`;
- `table_description_indexing` использует `embedder` и `document_store`;
- `sql_pairs_indexing` использует `embedder` и `document_store`;
- `instructions_indexing` использует `embedder` и `document_store`;
- `project_meta_indexing` использует только `document_store`.

Смысл этой группы в том, чтобы превратить текстовые или структурные артефакты в индексируемый контекст для retrieval.

### Группа 2. Retrieval

К этой группе относятся:

- `db_schema_retrieval`
- `historical_question_retrieval`
- `sql_pairs_retrieval`
- `instructions_retrieval`
- `sql_functions_retrieval`

Их роль - вернуть релевантный контекст к текущему вопросу.

Что видно по зависимости:

- `db_schema_retrieval` использует `llm`, `embedder`, `document_store`;
- `historical_question_retrieval` использует `embedder`, `document_store`;
- `sql_pairs_retrieval` использует `llm`, `embedder`, `document_store`;
- `instructions_retrieval` использует `embedder`, `document_store`;
- `sql_functions_retrieval` использует `engine` и `document_store`, причём engine здесь - `wren_ibis`.

Это показывает, что retrieval в проекте неоднороден: одни пайпы работают чисто через эмбеддинги, а другие дополнительно опираются на LLM или на Ibis-движок.

### Группа 3. Генерация и коррекция SQL

К этой группе относятся:

- `sql_generation`
- `sql_correction`
- `followup_sql_generation`
- `sql_regeneration`
- `question_recommendation_sql_generation`
- `sql_question_generation`
- `sql_generation_reasoning`
- `followup_sql_generation_reasoning`

Их роль - создавать SQL, исправлять его, генерировать продолжения и поддерживать reasoning-варианты генерации.

Что видно по зависимости:

- `sql_generation` использует `llm`, `engine`, `document_store`;
- `sql_correction` использует `llm`, `engine`, `document_store`;
- `followup_sql_generation` использует `llm`, `engine`, `document_store`;
- `sql_regeneration` использует `llm`, `engine`;
- `question_recommendation_sql_generation` использует `llm`, `engine`, `document_store`;
- `sql_question_generation` использует `llm`;
- `sql_generation_reasoning` использует `llm`;
- `followup_sql_generation_reasoning` использует `llm`.

Практический смысл этой группы в том, что именно она отвечает за превращение текста пользователя в SQL-результат и его возможную доработку.

### Группа 4. SQL-исполнение и постобработка

К этой группе относятся:

- `sql_executor`
- `preprocess_sql_data`
- `sql_answer`
- `sql_diagnosis`

Роль этой группы - довести SQL-цепочку до исполняемого и объяснимого результата.

Что видно по зависимости:

- `sql_executor` использует только `engine: wren_ui`;
- `preprocess_sql_data` использует только `llm`;
- `sql_answer` использует только `llm`;
- `sql_diagnosis` использует только `llm`.

Это показывает, что в проекте есть явное разделение между генерацией SQL и сопроводительной обработкой результата.

### Группа 5. Семантика, рекомендации и пользовательская помощь

К этой группе относятся:

- `semantics_description`
- `relationship_recommendation`
- `question_recommendation`
- `intent_classification`
- `misleading_assistance`
- `data_assistance`
- `user_guide_assistance`
- `chart_generation`
- `chart_adjustment`

Их роль - помогать системе объяснять сущности, классифицировать намерение пользователя, давать рекомендации и сопровождать ответ визуально или текстово.

Что видно по зависимости:

- `semantics_description` использует `llm`;
- `relationship_recommendation` использует `llm`;
- `question_recommendation` использует `llm`;
- `intent_classification` использует `llm`, `embedder`, `document_store`;
- `misleading_assistance` использует `llm`;
- `data_assistance` использует `llm`;
- `user_guide_assistance` использует `llm`;
- `chart_generation` использует `llm`;
- `chart_adjustment` использует `llm`.

Это не “дополнительные украшения”, а служебные этапы, которые помогают BI-контуру не ограничиваться только генерацией SQL.

### Группа 6. Извлечение таблиц и служебные индексы

К этой группе относятся:

- `sql_tables_extraction`

Роль этого pipe - извлечение таблиц, которые нужны для SQL-задачи.

По конфигу он использует только `llm`.

### Почему группировка pipeline важна

Если смотреть только на список имён, конфиг кажется длинным и разрозненным.
Но если разложить его по ролям, видно, что в системе есть несколько чётких контуров:

- индексация;
- поиск контекста;
- генерация SQL;
- коррекция и повторная генерация;
- выполнение SQL;
- вспомогательная семантика и рекомендации.

Это и есть функциональная карта BI-агента.

## Блок `settings`

### Поля и значения

- `doc_endpoint: https://docs.getwren.ai`
- `is_oss: true`
- `engine_timeout: 30`
- `column_indexing_batch_size: 50`
- `table_retrieval_size: 10`
- `table_column_retrieval_size: 100`
- `allow_intent_classification: true`
- `allow_sql_generation_reasoning: true`
- `allow_sql_functions_retrieval: true`
- `enable_column_pruning: false`
- `max_sql_correction_retries: 3`
- `query_cache_maxsize: 1000`
- `query_cache_ttl: 3600`
- `langfuse_host: https://cloud.langfuse.com`
- `langfuse_enable: false`
- `logging_level: INFO`
- `development: true`
- `historical_question_retrieval_similarity_threshold: 0.9`
- `sql_pairs_similarity_threshold: 0.7`
- `sql_pairs_retrieval_max_size: 10`
- `instructions_similarity_threshold: 0.7`
- `instructions_top_k: 10`

### Что означают эти настройки

`doc_endpoint` указывает на источник документации WrenAI.

`is_oss: true` говорит, что используется контур open source-версии.

`engine_timeout: 30` ограничивает ожидание ответа от движка.

`column_indexing_batch_size: 50` задаёт размер батча для индексирования колонок.

`table_retrieval_size: 10` ограничивает число таблиц, которые система поднимает на retrieval-этапе.

`table_column_retrieval_size: 100` задаёт верхнюю границу для выборки колонок.

`allow_intent_classification: true` включает классификацию намерения пользователя.

`allow_sql_generation_reasoning: true` включает reasoning-режим генерации SQL.

`allow_sql_functions_retrieval: true` разрешает retrieval по SQL-функциям.

`enable_column_pruning: false` отключает отдельную агрессивную обрезку колонок.

`max_sql_correction_retries: 3` ограничивает число попыток исправления SQL.

`query_cache_maxsize: 1000` и `query_cache_ttl: 3600` задают параметры кэша запросов.

`langfuse_host` и `langfuse_enable` задают подключение к Langfuse, но в текущем конфиге оно выключено.

`logging_level: INFO` задаёт уровень логирования.

`development: true` переводит систему в режим разработки.

Остальные параметры similarity threshold и top-k регулируют, насколько близкими должны быть документы для retrieval и сколько результатов поднимать.

### Практический смысл настроек

Эта секция не просто “подкручивает” интерфейс.
Она напрямую влияет на то, какие таблицы и документы попадут в контекст генерации SQL, как часто система будет пробовать исправлять ошибки и насколько масштабным будет retrieval.

## Как конфиг связан с остальным репозиторием

Этот файл не живёт отдельно.
Он должен быть согласован с:

- `bi-agent/semantic_layer/*.yaml`;
- `bi-agent/prompts/system_prompt.md`;
- `bi-agent/eval/questions.json`;
- `bi-agent/eval/run_eval.py`;
- `bi-agent/eval/golden.sql`;
- `bi-agent/eval/llm_judge.py`;
- `docs/data/marts.md`;
- `docs/data/schema.md`;
- контейнерным контуром в `docker-compose.yml`.

Если эти артефакты расходятся между собой, BI-контур начинает давать либо неполный, либо несогласованный результат.

## Текущее состояние

По фактическим файлам репозитория видно, что инфраструктурная часть уже собрана довольно подробно:

- LLM задан;
- embedder задан;
- движки заданы;
- Qdrant задан;
- pipeline перечислен и разбит на роли;
- настройки retrieval и коррекции тоже заданы.

Но рядом с этим есть и незаполненные BI-артефакты:

- `bi-agent/semantic_layer/models.yaml` пустой;
- `bi-agent/semantic_layer/metrics.yaml` пустой;
- `bi-agent/semantic_layer/joins.yaml` пустой;
- `bi-agent/prompts/system_prompt.md` пустой;
- `bi-agent/eval/golden.sql` пустой;
- `bi-agent/eval/llm_judge.py` пустой.

То есть инфраструктурный каркас уже есть, а содержательная часть BI-слоя ещё не доведена до полной завершённости.

## Целевое состояние

В целевом состоянии этот конфиг должен работать вместе с полностью заполненным семантическим слоем и согласованными BI-артефактами.
Тогда WrenAI будет опираться не только на инфраструктуру, но и на:

- явно описанные модели;
- согласованные метрики;
- формализованные связи;
- осмысленный системный промпт;
- более сильную оценку качества.

## Краткий вывод

`infra/wrenai/config.yaml` показывает, что BI-контур проекта уже собран на уровне системных компонентов и pipeline.
В нём есть языковая модель, эмбеддер, движки, Qdrant и настройки retrieval.
Но по текущему состоянию репозитория этот каркас ещё нужно синхронизировать с полностью заполненным семантическим слоем и со зрелым контуром оценки качества.

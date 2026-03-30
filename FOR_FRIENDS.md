# Max Chronicle For Friends

## Что Это Вообще Такое

Это не “ещё один агент ради агента”.

`Max Chronicle` — это local-first memory layer для агентов и живых проектов:

- хранит правду в локальной SQLite-базе
- умеет собирать стартовый контекст для нового агента
- держит readable SSOT-доки рядом с кодом
- поддерживает snapshots, timeline и durable events
- может сверху подключать semantic recall, но не делает его source of truth

Нормальным языком:
это штука, чтобы твои агенты не жили в амнезии, не путали “что реально было” с “что кто-то когда-то нагенерил”, и могли подниматься в новый тред без шаманства.

## Почему Это Может Быть Полезно

Если коротко:

- у тебя есть несколько проектов, и контекст постоянно расползается
- у тебя есть несколько агентов, и каждый начинает с нуля
- у тебя есть решения, баги, инсайты, статусы, которые хочется не терять
- хочется timeline, snapshots и понятный source of truth, а не кашу из чатов

Вот для этого и нужен Chronicle.

## Что Внутри Архива

- исходники пакета `max_chronicle`
- install docs
- portable wrapper scripts
- пример готового workspace

То есть это не сырой слепок моей личной машины.
Это уже нормальная shareable сборка, которую можно поставить у себя и адаптировать под свой стек.

## Самый Быстрый Старт

1. Распакуй архив.
2. Открой папку с исходниками.
3. Создай venv и установи пакет:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

4. Создай свой workspace:

```bash
chronicle init --root ~/chronicle-workspace
export CHRONICLE_ROOT=~/chronicle-workspace
```

5. Проверь, что всё живо:

```bash
chronicle status
chronicle startup --domain global --format json
chronicle activate --domain global
```

Если это работает — всё, ядро стоит нормально.

## Как Этим Пользоваться По-Человечески

Базовые команды:

```bash
chronicle record "We changed X because Y" --category decision --project my-project
chronicle recent --limit 10
chronicle timeline --at "2026-03-26T12:00:00+01:00"
chronicle capture-runtime --domain global
chronicle query "current blockers" --domain global --format json
```

MCP-сервер:

```bash
chronicle-mcp --profile chronicler
chronicle-mcp-readonly
chronicle-mcp-chronicler
```

## Важная Мысль

Chronicle лучше всего работает, когда ты не пытаешься сразу поднять весь космодром.

Правильный порядок такой:

1. Сначала подними `core`.
2. Убедись, что `record / recent / startup / query` работают.
3. Только потом подключай optional integrations.

## Что Здесь Optional

Можно вообще не трогать на старте:

- Mem0 / semantic recall
- launchd automation
- repo hooks
- внешние дайджесты / runtime feeds
- model-backed canaries

Core уже полезен сам по себе.

## Если Хочешь Быстро Понять Архитектуру

Смотри в таком порядке:

1. `FOR_FRIENDS.md`
2. `INSTALL.md`
3. `max_chronicle/config.py`
4. `max_chronicle/cli.py`
5. `max_chronicle/service.py`
6. example workspace

## Коротко В Одну Строку

Это durable memory OS для агентов:
не магия, не игрушка, а способ перестать терять контекст и начать работать с историей, состоянием и решениями как взрослый человек.

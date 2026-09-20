# Трекер задач: Gitea

Issues и спеки этого репо живут в Gitea:
`https://git.dementev.space/ddmitry/airflow-greenplum`.
Все операции — через CLI [`tea`](https://gitea.com/gitea/tea).
`gh` и `glab` с этим хостом не работают.

`tea` сам определяет репо и логин по `git remote -v`, если запущен внутри клона
(логин `dementev`, пользователь `ddmitry`). Для машиночитаемого вывода
добавляйте `-o json` к любой команде `list`.

## Команды

- **Создать issue**: `tea issues create --title "..." --description "..."`. Для многострочного описания используйте heredoc или `"$(cat file.md)"`. `--labels a,b` вешает метки сразу при создании.
- **Прочитать issue**: `tea issues <номер> --comments`.
- **Список issues**: `tea issues list --state open -o json --fields index,title,state,labels,assignees,body`. Фильтры: `--labels`, `--state all|open|closed`, `--keyword`, `--assignee`.
- **Прокомментировать**: `tea comments add <номер> "..."` (короткая форма: `tea comment <номер> "..."`).
- **Добавить / снять метки**: `tea issues edit <номер> --add-labels "a,b"` / `--remove-labels "a,b"`. Метка должна существовать в репо: `tea labels create --name "..." --color "..." --description "..."`.
- **Закрыть**: `tea issues close <номер>`. Комментарий при закрытии команда не принимает — сначала `tea comments add`, потом `close`.
- **Pull requests**: `tea pulls create`, `tea pulls <номер>`, `tea pulls list`, `tea pulls merge` — тот же набор, что у `tea issues`. Комментарии к PR — через тот же `tea comments add <номер>`.

## PR как источник запросов

**PR как источник запросов: нет.** _(Поставьте `да`, если внешние PR нужно рассматривать как feature request; `/triage` читает этот флаг.)_

Если стоит `да`, PR проходят те же метки и состояния, что и issues, через `tea pulls`:

- **Прочитать PR**: `tea pulls <номер> --comments`; diff — `tea pulls checkout <номер>` и `git diff main...`.
- **Список внешних PR для триажа**: `tea pulls list --state open -o json --fields index,title,author,labels,body`, дальше оставить только PR от авторов, которые не являются участниками репо.
- **Комментарий / метки / закрытие**: `tea comments add`, `tea issues edit --add-labels`/`--remove-labels` (работает и для номеров PR), `tea pulls close`.

В Gitea issues и PR нумеруются в одном пространстве: `#42` может быть и тем, и другим. `tea issues 42` показывает поле `kind`; ещё можно смотреть `tea issues list --kind all`.

## Когда скилл говорит «опубликовать в трекер»

Создать issue в Gitea: `tea issues create`.

## Когда скилл говорит «взять тикет»

Выполнить `tea issues <номер> --comments`.

## Операции wayfinding

Используется `/wayfinder`. **Карта** — одна issue, **дочерние** issues — тикеты.

- **Карта**: одна issue с меткой `wayfinder:map`, в теле — Notes / Decisions-so-far / Fog. `tea issues create --labels wayfinder:map`.
- **Дочерний тикет**: issue со строкой `Part of #<карта>` в начале описания и меткой `wayfinder:<тип>` (`research`/`prototype`/`grilling`/`task`). После взятия в работу тикет назначается на исполнителя.
- **Блокировки**: в Gitea есть встроенные зависимости между issues, но `tea` их не поддерживает. Используйте строку `Blocked by: #<n>, #<n>` в начале описания дочернего тикета. Тикет разблокирован, когда все блокирующие закрыты.
- **Поиск фронтира**: `tea issues list --state open --labels wayfinder:task -o json` (повторить для каждого типа), оставить только детей этой карты, отбросить те, у кого есть открытый блокер в `Blocked by` или назначенный исполнитель; первый по порядку в карте.
- **Взять в работу**: `tea issues edit <n> --add-assignees ddmitry` — первая запись в сессии.
- **Закрыть**: `tea comments add <n> "<ответ>"`, затем `tea issues close <n>`, затем дописать указатель на контекст (суть + ссылка) в Decisions-so-far карты.

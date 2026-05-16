# Tools And Capabilities

Этот файл сгенерирован автоматически. Перед выполнением задачи опирайся на него как на фактический список доступных инструментов.

## Доступные команды
- sh: /usr/bin/sh
- bash: /usr/bin/bash
- python: /usr/local/bin/python
- python3: /usr/local/bin/python3
- pip: /usr/local/bin/pip
- pip3: /usr/local/bin/pip3
- apt: /usr/bin/apt
- apt-get: /usr/bin/apt-get
- dpkg: /usr/bin/dpkg
- curl: /usr/bin/curl
- git: /usr/bin/git
- grep: /usr/bin/grep
- sed: /usr/bin/sed
- awk: /usr/bin/awk
- find: /usr/bin/find
- tar: /usr/bin/tar
- gzip: /usr/bin/gzip
- service: /usr/sbin/service

## Недоступные команды
- wget: недоступно
- ss: недоступно
- netstat: недоступно
- ps: недоступно
- nginx: недоступно
- systemctl: недоступно
- supervisorctl: недоступно
- docker: недоступно
- docker-compose: недоступно

## Проверенные пакеты
- curl: installed
- git: installed

## Отсутствующие или неизвестные пакеты
- nginx: not-installed
- wget: not-installed
- procps: not-installed
- iproute2: not-installed

## Правила использования
- Если задача требует недоступного инструмента, используй доступную альтернативу.
- Если что-то невозможно сделать без команд из недоступного списка, то сначала установи то чего не хватает через стандартный пакетный менеджер (если невозможно, то через curl (если curl не установлен, то установи его тоже))
- Если задача сформулирована в том числе в том, чтобы установить недостающую команду, то установи её
- Если безопасной альтернативы нет, верни БЛОКЕР.

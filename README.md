# Korneki Bot

Telegram-бот для бизнеса Караганды — помощь по языковому законодательству, готовые переводы и консультации.

## Возможности

- Двуязычный интерфейс (казахский / русский)
- Законодательство о языках, рекламе, защите прав потребителей, КоАП
- Call-center — бесплатный перевод через WhatsApp
- Готовые переводы для 6 сфер бизнеса (торговля, автосервис, стройматериалы, товары для дома, косметология, меню)
- Контакты специалистов акимата города Караганды

## Технологии

- Python 3.10+
- [aiogram 3.x](https://docs.aiogram.dev/) — асинхронный Telegram Bot API
- python-dotenv — переменные окружения

## Быстрый запуск

```bash
# Клонировать репозиторий
git clone https://github.com/YOUR_USERNAME/korneki_bot.git
cd korneki_bot

# Создать виртуальное окружение
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Установить зависимости
pip install -r requirements.txt

# Настроить токен
cp .env.example .env
# Отредактировать .env — указать BOT_TOKEN от @BotFather

# Запустить
python bot.py
```

## Структура проекта

```
korneki_bot/
├── bot.py               # Основной файл бота
├── data/
│   └── bot_content.py   # Тексты, переводы, контент
├── tutor/               # Справочные документы
├── requirements.txt     # Зависимости
├── .env.example         # Шаблон переменных окружения
└── .gitignore
```

## Команды бота

| Команда  | Описание             |
|----------|----------------------|
| `/start` | Начать / Бастау      |
| `/menu`  | Главное меню / Мәзір |
| `/help`  | Помощь / Көмек       |

## Деплой

Бот работает в режиме long polling — достаточно запустить `python bot.py` на любом сервере с Python 3.10+.

Для продакшена рекомендуется использовать systemd, Docker или PaaS (Railway, Render).

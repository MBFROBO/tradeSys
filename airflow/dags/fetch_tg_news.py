from common.parserFunction import main
import pandas as pd
import re
import os
import asyncio
from datetime import datetime, date, timedelta

from collections import Counter
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator


channel_list = os.getenv("CHANNELS", "")
fetch_limit_days = int(os.getenv("FETCH_LIMIT_DAYS", "1"))
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres-data:5432/airflow_data")

chs = re.split(r"[,\s]+", channel_list.strip())
if not chs:
    raise RuntimeError(f"Нет каналов для парсинга новостей: {__file__} | CHANNELS='{channel_list}'")



# --- Регексы  ---
RE_SCRIPT = re.compile(r'<(script|style).*?</\1>', re.S | re.I)
RE_TAGS = re.compile(r'<[^>]+>')
RE_MD_BI = re.compile(r'(\*{1,2}|_{1,2})(.*?)\1')
RE_MD_LINK = re.compile(r'\[([^\]]+)\]\((?:https?://|www\.)[^\)]+\)')
RE_URL = re.compile(r'(https?://\S+|www\.\S+)')
RE_HASH_CAPTURE = re.compile(r'#([\w\-]+)', re.UNICODE)
RE_HASH = re.compile(r'#\w+', flags=re.UNICODE)
RE_MENTION = re.compile(r'@\w+')
RE_EMOJI = re.compile("[" 
    u"\U0001F1E0-\U0001F1FF"
    u"\U0001F300-\U0001F6FF"
    u"\U0001F700-\U0001F77F"
    u"\U0001F780-\U0001F7FF"
    u"\U0001F800-\U0001F8FF"
    u"\U0001F900-\U0001F9FF"
    u"\U0001FA00-\U0001FAFF"
    u"\u2600-\u26FF"
    u"\u2700-\u27BF"
    "]+", flags=re.UNICODE)
RE_SEP_LINE = re.compile(r'^\s*[-—–_]{3,}\s*$', re.M)
RE_WS = re.compile(r'[ \t]+')


def _strip_markdown(text: str) -> str:
    text = RE_MD_BI.sub(lambda m: m.group(2), text)
    text = RE_MD_LINK.sub(lambda m: m.group(1), text)
    return text


def basic_clean(text: str) -> Optional[str]:
    """
    Чистит сырую новость из телеги:
    - убирает html/markdown/ссылки/@каналы/эмодзи
    - хэштеги выносит в конец как метки темы
    - нормализует пробелы
    - отбрасывает слишком короткий шум
    Возвращает строку или None (если бесполезный короткий пост)
    """
    if not text:
        return None

    t = text

    # 1 Удаляем <script>...</script>, <style>...</style> и любые html-теги
    t = RE_SCRIPT.sub(" ", t)
    t = RE_TAGS.sub(" ", t)

    # 2 Удаляем markdown-разметку (**жирный**, _курсив_, [текст](url))
    t = _strip_markdown(t)

    # 3 Забираем хэштеги как темы
    raw_tags = RE_HASH_CAPTURE.findall(t)  
    tags = []
    for tag in raw_tags:
        clean_tag = re.sub(r"[^0-9A-Za-zА-Яа-яЁё]+", " ", tag).strip().lower()
        if clean_tag and clean_tag not in tags:
            tags.append(clean_tag)

    # 4 Удаляем URLs, #теги в тексте, упоминания @user, эмодзи, разделители
    t = RE_URL.sub(" ", t)
    t = RE_HASH.sub(" ", t)
    t = RE_MENTION.sub(" ", t)
    t = RE_EMOJI.sub(" ", t)
    t = RE_SEP_LINE.sub(" ", t)

    # 5 Чистим лишние переносы строк и пробелы
    #    - режем по строкам
    #    - убираем пустые строки
    #    - склеиваем обратно в один абзац
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    t = " ".join(lines)

    # заменяем подряд идущие табы/пробелы на один пробел
    t = RE_WS.sub(" ", t).strip()

    # иногда остаются невидимые модификаторы эмодзи (U+FE0F),
    # просто выбрасываем
    t = t.replace("\uFE0F", "")

    # 6 Приклеиваем темы (хэштеги) в конец
    #    формируем что-то вроде: "... . #санкции,нефть"
    if tags:
        t = f"{t}. #{tags[0]}" + ("," + ",".join(tags[1:]) if len(tags) > 1 else "")

    # 7 Отфильтровать пустой и слишком короткий мусор
    #    если текста меньше 40 символов — это обычно спам
    if len(t) < 40:
        return None

    return t


STOPWORDS_CUSTOM = {
    # предлоги / союзы / служебка
    "в", "на", "и", "по", "с", "что", "не", "за", "из", "о", "об", "для",
    "к", "от", "как", "это", "а", "до", "также", "при", "со", "их",
    "но", "все", "чтобы", "у", "мы", "том", "под", "ее", "если",
    "может", "еще", "они", "то", "она", "во", "словам", "этого",
    "или", "так", "могут", "того", "только", "были", "который",
    "где", "быть", "два", "один", "я", "чем", "уже", "будет",
    "будут", "был", "будут", "после", "раньше", "ранее",
    "через", "более", "только", "около", "без",

    # местоимения 
    "он", "его", "её", "ее", "их", "она", "мы", "я", "они",

    # репортёрские слова
    "заявил", "сообщили", "сообщил", "сообщает", "сообщила",
    "отметил", "следить", "новостями", "пишет",
    "пресс", "сказал", "рассказал", "передает", "данным",
    "картина", "канале", "подпишись", "читать", "ссылкой",
    "новости",

    # оформление канала / платформа
    "тасс", "рбк", "telegram", "телеграм",
    "видео", "фото", "getty", "images",

    # время
    "года", "году", "дня", "время", "октября",
    "лет",

    # общие слова-связки
    "этом", "там", "том", "того", "так", "ли", "уже",
    "будет", "будут", "может", "могут",
}



def tg_news_parce():
    """Парсит новости из телеги, чистит и сохраняет в БД."""
    out = asyncio.run(main(chs, date_str=(datetime.now().date() - pd.Timedelta(days=fetch_limit_days)).isoformat()))
    rows = []
    for ch in chs:
        for dt, text in out.get(ch, []):
            rows.append({
                "Date":dt,
                "news":text,
                "source":ch,
            })

    df = pd.DataFrame(rows)

    df["Date"] = pd.to_datetime(df["Date"], utc=True)

    text_series = df["news"].fillna("")

    text_series = text_series.str.lower()

    text_series = text_series.apply(basic_clean)
    text_series = text_series.dropna()

    df["clean_news"] = text_series
    df = df.dropna(subset=["clean_news"]).reset_index(drop=True)

    for idx, text in df["clean_news"].items():
        text = re.findall(r"\w+", text)
        text = [word for word in text if word not in STOPWORDS_CUSTOM and len(word) > 2]
        text = " ".join(text)
        df.loc[idx, 'clean_news'] = text


    df.to_sql("tg_news", con=db_url, if_exists="replace", index=False)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(hours=12),
}

with DAG(
    dag_id="tg_news_parce_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:
    train_task = PythonOperator(
        task_id="tg_news_parce",
        python_callable=tg_news_parce,
    )
import json
import logging
import os
import sys
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def load_questions(path):
    """Читает вопросы из JSON."""
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def build_api_url():
    """Формирует URL для WrenAI API."""
    base_url = os.getenv("WREN_AI_ENDPOINT")
    if not base_url:
        logging.error("Missing env var: WREN_AI_ENDPOINT")
        sys.exit(1)

    path = os.getenv("WREN_AI_QUERY_PATH", "/api/v1/query")
    if base_url.endswith("/") and path.startswith("/"):
        return f"{base_url[:-1]}{path}"
    if not base_url.endswith("/") and not path.startswith("/"):
        return f"{base_url}/{path}"
    return f"{base_url}{path}"


def extract_sql(response_json):
    """Извлекает SQL из ответа WrenAI."""
    if isinstance(response_json, dict):
        for key in ("sql", "query", "generated_sql"):
            if response_json.get(key):
                return response_json[key]
        for container_key in ("data", "result", "payload"):
            container = response_json.get(container_key)
            if isinstance(container, dict):
                for key in ("sql", "query", "generated_sql"):
                    if container.get(key):
                        return container[key]
    return None


def request_sql(api_url, question_text, timeout=30):
    """Отправляет вопрос в WrenAI и получает SQL."""
    payload = {"question": question_text}
    data = json.dumps(payload).encode("utf-8")
    request = Request(
        api_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        response_json = json.loads(body)
    return extract_sql(response_json)


def main():
    """Запускает базовую проверку вопросов к BI-агенту."""
    questions_path = Path(__file__).with_name("questions.json")
    questions = load_questions(questions_path)
    api_url = build_api_url()

    for item in questions:
        question_id = item.get("id", "unknown")
        question_text = item.get("question", "")
        target_mart = item.get("target_mart", "")

        try:
            sql = request_sql(api_url, question_text)
        except (HTTPError, URLError, json.JSONDecodeError) as exc:
            logging.error("fail %s: %s", question_id, exc)
            continue

        if not sql:
            logging.error("fail %s: empty sql", question_id)
            continue

        if target_mart.lower() in sql.lower():
            logging.info("pass %s", question_id)
        else:
            logging.error("fail %s: target mart not found", question_id)


if __name__ == "__main__":
    main()

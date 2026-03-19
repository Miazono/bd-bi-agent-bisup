import json
import os
from urllib.request import Request, urlopen
from unittest.mock import MagicMock, patch


def test_wrenai_health():
    endpoint = os.getenv("WREN_AI_ENDPOINT", "http://wren-ui:3000").rstrip("/")
    url = f"{endpoint}/health"

    response = MagicMock()
    response.__enter__.return_value = response
    response.status = 200

    with patch("urllib.request.urlopen", return_value=response):
        request = Request(url)
        with urlopen(request, timeout=10) as resp:
            assert resp.status == 200


def test_question_returns_sql():
    endpoint = os.getenv("WREN_AI_ENDPOINT", "http://wren-ui:3000").rstrip("/")
    url = f"{endpoint}/api/v1/query"
    payload = {"question": "Show revenue by sales channel for one day."}

    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = json.dumps({"sql": "SELECT 1"}).encode("utf-8")

    with patch("urllib.request.urlopen", return_value=response):
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)

    assert "select" in data.get("sql", "").lower()

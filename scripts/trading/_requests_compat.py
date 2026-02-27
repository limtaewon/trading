"""requests 호환 모듈 — 표준 라이브러리(urllib)만 사용

cron 환경에서 pip 패키지(requests)가 없을 때 자동 fallback.
사용법:
    try:
        import requests
    except ImportError:
        from _requests_compat import requests

지원 API:
    requests.get(url, params=..., headers=..., timeout=...)
    requests.post(url, data=..., json=..., headers=..., timeout=...)
    resp.status_code, resp.text, resp.json(), resp.raise_for_status()
"""

import json as _json
import urllib.request
import urllib.parse
import urllib.error


class _Response:
    """requests.Response 호환 객체"""

    def __init__(self, http_response):
        self.status_code = http_response.status
        self._body = http_response.read()
        self.headers = dict(http_response.headers)

    @property
    def text(self):
        return self._body.decode("utf-8", errors="replace")

    def json(self):
        return _json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise urllib.error.HTTPError(
                url="", code=self.status_code, msg=f"HTTP {self.status_code}",
                hdrs=None, fp=None,
            )


class _Session:
    """requests 모듈 호환 인터페이스"""

    @staticmethod
    def get(url, params=None, headers=None, timeout=30, **_kw):
        if params:
            url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, method="GET")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            return _Response(resp)
        except urllib.error.HTTPError as e:
            r = _Response(e)
            return r

    @staticmethod
    def post(url, data=None, json=None, headers=None, timeout=30, **_kw):
        h = dict(headers or {})
        if json is not None:
            body = _json.dumps(json).encode("utf-8")
            h.setdefault("Content-Type", "application/json")
        elif data is not None:
            body = data if isinstance(data, bytes) else data.encode("utf-8")
        else:
            body = None
        req = urllib.request.Request(url, data=body, method="POST")
        for k, v in h.items():
            req.add_header(k, v)
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            return _Response(resp)
        except urllib.error.HTTPError as e:
            r = _Response(e)
            return r


# 모듈처럼 사용: from _requests_compat import requests
requests = _Session()

"""ツイートURLまわりの純粋なヘルパー(Discord / DB に依存しない)。"""
from __future__ import annotations

import re
from typing import Iterable, Optional

# twitter.com / x.com / 既に fx 系に変換済みのドメインも拾う
_STATUS_RE = re.compile(
    r"https?://(?:www\.|mobile\.)?"
    r"(?:twitter\.com|x\.com|fxtwitter\.com|vxtwitter\.com|fixupx\.com|fixvx\.com)"
    r"/(?P<user>[^/?#]+)/status(?:es)?/(?P<id>\d+)"
)


def extract_status_id(link: str) -> Optional[str]:
    """ツイートURLからステータスIDを取り出す。ツイートURLでなければ None。"""
    m = _STATUS_RE.search(link or "")
    return m.group("id") if m else None


def to_fixed_url(link: str, domain: str = "fxtwitter.com") -> str:
    """twitter.com / x.com のツイートURLを FixTweet 系ドメインに差し替える。

    Discord が画像・動画・引用・翻訳付きの綺麗な埋め込みを作ってくれるようになる。
    ツイートURLでなければ元のリンクをそのまま返す。
    追跡用のクエリ(?s=20 など)は落とす。
    """
    m = _STATUS_RE.search(link or "")
    if not m:
        return link
    return f"https://{domain}/{m.group('user')}/status/{m.group('id')}"


def entry_key(link: str, fallback_id: Optional[str] = None) -> str:
    """既読管理用のキー。ツイートならステータスID、それ以外はリンク(またはentry id)。"""
    return extract_status_id(link) or fallback_id or link


def order_oldest_first(entries: Iterable) -> list:
    """RSSは新しい順に並ぶので、古い順(投稿順)に並べ直す。

    全件にステータスIDがあればIDの数値順(Snowflakeは時系列順)で並べる。
    """
    items = list(reversed(list(entries)))
    ids = [extract_status_id(getattr(e, "link", "")) for e in items]
    if items and all(ids):
        items = [e for _, e in sorted(zip(map(int, ids), items), key=lambda p: p[0])]
    return items

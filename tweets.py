"""ツイートURLまわりの純粋なヘルパー(Discord / DB に依存しない)。"""
from __future__ import annotations

import re
from typing import Iterable, Optional

# ホストは問わない: twitter.com / x.com / fx系 / Nitter(任意のインスタンス)すべて
# 「/ユーザー名/status/数字」の形をツイートURLとみなす
_STATUS_RE = re.compile(
    r"https?://[^/\s]+/(?P<user>[^/?#\s]+)/status(?:es)?/(?P<id>\d+)"
)


def extract_status_id(link: str) -> Optional[str]:
    """ツイートURLからステータスIDを取り出す。ツイートURLでなければ None。"""
    m = _STATUS_RE.search(link or "")
    return m.group("id") if m else None


def to_fixed_url(link: str, domain: str = "fxtwitter.com") -> str:
    """ツイートURL(twitter.com / x.com / Nitter など)を FixTweet 系ドメインに差し替える。

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


# --- リツイート / 返信の判定(タイトルの書き出しで見分ける) ---
# Nitter: 「RT by @アカウント: 本文」「R to @相手: 本文」
# RSS.app など: 「RT @元アカウント: 本文」「@相手 本文」
_RT_RE = re.compile(r"^\s*RT\s+(?:by\s+)?@", re.IGNORECASE)
_REPLY_RE = re.compile(r"^\s*(?:R\s+to\s+@|@\w)", re.IGNORECASE)


def is_retweet(title: str) -> bool:
    return bool(_RT_RE.match(title or ""))


def is_reply(title: str) -> bool:
    return bool(_REPLY_RE.match(title or ""))


def should_skip(title: str, skip_retweets: bool = True, skip_replies: bool = True) -> bool:
    return (skip_retweets and is_retweet(title)) or (skip_replies and is_reply(title))

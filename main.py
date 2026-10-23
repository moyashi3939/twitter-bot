import asyncio
import logging
import os
from typing import List, Optional

import aiohttp
import asyncpg
import discord
import feedparser
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

from tweets import entry_key, order_oldest_first, to_fixed_url

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
# FixTweet 系ドメイン。fxtwitter.com / fixupx.com / vxtwitter.com など
FIX_DOMAIN = os.getenv("FIX_DOMAIN", "fxtwitter.com")
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "5"))
# 1フィード・1回のチェックで送る最大件数(超えた分は次回に持ち越す。取りこぼしはしない)
MAX_SEND_PER_CYCLE = int(os.getenv("MAX_SEND_PER_CYCLE", "20"))
# 連投時の送信間隔(秒)。Discordのレート制限と通知のうるささ対策
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1.0"))
# 既読として DB に残す件数(フィードごと)
KEEP_SEEN = 500

log = logging.getLogger("rssbot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; rss-discord-bot/2.0)"}


async def fetch_entries(session: aiohttp.ClientSession, url: str) -> list:
    """RSSを取得してエントリ一覧(新しい順)を返す。失敗時は例外。"""
    async with session.get(url) as resp:
        resp.raise_for_status()
        xml_data = await resp.text()
    # feedparser は同期処理なのでスレッドに逃がす
    parsed = await asyncio.to_thread(feedparser.parse, xml_data)
    return list(parsed.entries)


def key_of(entry) -> str:
    return entry_key(getattr(entry, "link", ""), getattr(entry, "id", None))


class RSSBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=discord.Intents.default())
        self.pool: Optional[asyncpg.Pool] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._check_lock = asyncio.Lock()

    # ------------------------------------------------------------------ setup
    async def setup_hook(self):
        self.pool = await asyncpg.create_pool(DATABASE_URL)
        self.session = aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HTTP_HEADERS)

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rss_feeds (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    channel_id BIGINT,
                    rss_url TEXT,
                    last_entry_link TEXT
                )
                """
            )
            # 既存DBとの互換: 新カラムは追加するだけ
            await conn.execute(
                "ALTER TABLE rss_feeds ADD COLUMN IF NOT EXISTS initialized BOOLEAN NOT NULL DEFAULT FALSE"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_entries (
                    feed_id INTEGER NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
                    entry_id TEXT NOT NULL,
                    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (feed_id, entry_id)
                )
                """
            )

        self.check_rss.start()
        await self.tree.sync()

    async def close(self):
        self.check_rss.cancel()
        if self.session:
            await self.session.close()
        if self.pool:
            await self.pool.close()
        await super().close()

    # ------------------------------------------------------------- notification
    async def _resolve_channel(self, channel_id: int):
        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden):
                return None
        return channel

    async def _mark_seen(self, conn, feed_id: int, keys: List[str]):
        if not keys:
            return
        await conn.executemany(
            "INSERT INTO seen_entries (feed_id, entry_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            [(feed_id, k) for k in keys],
        )

    async def _prune_seen(self, conn, feed_id: int):
        await conn.execute(
            """
            DELETE FROM seen_entries
            WHERE feed_id = $1 AND entry_id NOT IN (
                SELECT entry_id FROM seen_entries WHERE feed_id = $1
                ORDER BY seen_at DESC LIMIT $2
            )
            """,
            feed_id,
            KEEP_SEEN,
        )

    async def check_feed(self, feed) -> None:
        feed_id = feed["id"]
        rss_url = feed["rss_url"]

        entries = await fetch_entries(self.session, rss_url)
        if not entries:
            return

        async with self.pool.acquire() as conn:
            seen = {
                r["entry_id"]
                for r in await conn.fetch("SELECT entry_id FROM seen_entries WHERE feed_id = $1", feed_id)
            }

            # --- 初回(または旧バージョンから移行した)フィードの初期化 ---
            if not feed["initialized"]:
                last_link = feed["last_entry_link"]
                links = [getattr(e, "link", None) for e in entries]
                if last_link and last_link in links:
                    # 旧バージョンが最後に通知した位置より新しいものは「未通知」として扱う
                    already = entries[links.index(last_link):]
                else:
                    already = entries
                await self._mark_seen(conn, feed_id, [key_of(e) for e in already])
                await conn.execute("UPDATE rss_feeds SET initialized = TRUE WHERE id = $1", feed_id)
                seen |= {key_of(e) for e in already}

            # --- 新着だけ取り出して、古い順に並べる(公式ツイートの連投対策) ---
            fresh = [e for e in entries if key_of(e) not in seen]
            if not fresh:
                return
            fresh = order_oldest_first(fresh)

            channel = await self._resolve_channel(feed["channel_id"])
            if channel is None:
                log.warning("channel %s not found (feed %s)", feed["channel_id"], feed_id)
                return

            backlog = max(0, len(fresh) - MAX_SEND_PER_CYCLE)
            to_send = fresh[:MAX_SEND_PER_CYCLE]
            log.info("feed %s: %d new (%d deferred)", feed_id, len(to_send), backlog)

            sent_any = False
            for i, entry in enumerate(to_send):
                link = to_fixed_url(getattr(entry, "link", ""), FIX_DOMAIN)
                try:
                    await channel.send(link)
                except discord.Forbidden:
                    log.warning("no permission to send in channel %s (feed %s)", channel.id, feed_id)
                    break
                except discord.HTTPException as e:
                    log.warning("send failed (feed %s): %s", feed_id, e)
                    break
                # 送れた分だけ即座に既読にする → 途中で落ちても重複送信しない
                await self._mark_seen(conn, feed_id, [key_of(entry)])
                sent_any = True
                if i < len(to_send) - 1:
                    await asyncio.sleep(SEND_INTERVAL)

            if sent_any:
                await self._prune_seen(conn, feed_id)

    async def process_rss_check(self):
        if self._check_lock.locked():
            return  # 前回の連投送信がまだ終わっていなければ重複実行しない
        async with self._check_lock:
            async with self.pool.acquire() as conn:
                feeds = await conn.fetch(
                    "SELECT id, channel_id, rss_url, last_entry_link, initialized FROM rss_feeds"
                )

            sem = asyncio.Semaphore(5)

            async def run(feed):
                async with sem:
                    try:
                        await self.check_feed(feed)
                    except Exception as e:  # 1フィードの失敗で全体を止めない
                        log.warning("RSS check error (%s): %s", feed["rss_url"], e)

            await asyncio.gather(*(run(f) for f in feeds))

    @tasks.loop(minutes=POLL_MINUTES)
    async def check_rss(self):
        await self.process_rss_check()

    @check_rss.before_loop
    async def before_check_rss(self):
        await self.wait_until_ready()

    @check_rss.error
    async def check_rss_error(self, error: BaseException):
        log.exception("check_rss crashed, restarting", exc_info=error)
        if not self.check_rss.is_running():
            self.check_rss.restart()


bot = RSSBot()


@bot.event
async def on_ready():
    log.info("Logged in as %s", bot.user)


# ---------------------------------------------------------------------- permissions
async def is_admin_or_owner(interaction: discord.Interaction) -> bool:
    """Botのオーナー、または「サーバーの管理」権限を持つ人だけ許可する。"""
    if await bot.is_owner(interaction.user):
        return True
    perms = interaction.permissions
    if perms.manage_guild or perms.administrator:
        return True
    raise app_commands.CheckFailure("このコマンドはBotのオーナーかサーバー管理者のみ使えます。")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        msg = str(error) or "このコマンドを使う権限がありません。"
    else:
        log.exception("command error", exc_info=error)
        msg = "エラーが発生しました。"
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


# ---------------------------------------------------------------------- commands
def _can_post(channel: discord.TextChannel) -> Optional[str]:
    me = channel.guild.me
    perms = channel.permissions_for(me)
    missing = [
        name
        for name, ok in (
            ("チャンネルを見る", perms.view_channel),
            ("メッセージを送信", perms.send_messages),
            ("埋め込みリンク", perms.embed_links),
        )
        if not ok
    ]
    return "、".join(missing) if missing else None


@bot.tree.command(name="follow", description="RSSフィードをチャンネルに登録します")
@app_commands.describe(rss_url="RSS.appで生成したURL", channel="通知を送るチャンネル")
@app_commands.guild_only()
@app_commands.check(is_admin_or_owner)
async def follow(interaction: discord.Interaction, rss_url: str, channel: discord.TextChannel):
    rss_url = rss_url.strip()
    if not rss_url.startswith(("http://", "https://")):
        await interaction.response.send_message("URLは http:// か https:// で始まるものを指定してください。", ephemeral=True)
        return

    missing = _can_post(channel)
    if missing:
        await interaction.response.send_message(
            f"{channel.mention} でBotに次の権限がありません: {missing}", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        entries = await fetch_entries(bot.session, rss_url)
    except Exception as e:
        await interaction.followup.send(f"RSSを取得できませんでした: `{e}`", ephemeral=True)
        return

    async with bot.pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM rss_feeds WHERE guild_id = $1 AND channel_id = $2 AND rss_url = $3",
            interaction.guild_id,
            channel.id,
            rss_url,
        )
        if exists:
            await interaction.followup.send("すでに同じ組み合わせで登録されています。", ephemeral=True)
            return

        feed_id = await conn.fetchval(
            """
            INSERT INTO rss_feeds (guild_id, channel_id, rss_url, last_entry_link, initialized)
            VALUES ($1, $2, $3, $4, TRUE) RETURNING id
            """,
            interaction.guild_id,
            channel.id,
            rss_url,
            entries[0].link if entries else None,
        )
        # 登録時点の投稿は既読にしておく(過去ツイートが一気に流れないように)
        await bot._mark_seen(conn, feed_id, [key_of(e) for e in entries])

    await interaction.followup.send(f"{channel.mention} に {rss_url} の通知を登録しました!", ephemeral=True)


async def _feed_choices(interaction: discord.Interaction, current: str):
    async with bot.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT rss_url FROM rss_feeds WHERE guild_id = $1 GROUP BY rss_url ORDER BY rss_url",
            interaction.guild_id,
        )
    return [
        app_commands.Choice(name=r["rss_url"][-100:], value=r["rss_url"][:100])
        for r in rows
        if current.lower() in r["rss_url"].lower()
    ][:25]


@bot.tree.command(name="unfollow", description="登録したRSSフィードを解除します")
@app_commands.describe(rss_url="解除するRSSのURL", channel="指定しない場合は、このサーバーの全チャンネルから解除")
@app_commands.autocomplete(rss_url=_feed_choices)
@app_commands.guild_only()
@app_commands.check(is_admin_or_owner)
async def unfollow(
    interaction: discord.Interaction, rss_url: str, channel: Optional[discord.TextChannel] = None
):
    async with bot.pool.acquire() as conn:
        if channel:
            result = await conn.execute(
                "DELETE FROM rss_feeds WHERE guild_id = $1 AND rss_url = $2 AND channel_id = $3",
                interaction.guild_id,
                rss_url,
                channel.id,
            )
        else:
            result = await conn.execute(
                "DELETE FROM rss_feeds WHERE guild_id = $1 AND rss_url = $2",
                interaction.guild_id,
                rss_url,
            )
    count = int(result.split()[-1])
    msg = f"{count}件の登録を解除しました。" if count else "該当する登録が見つかりませんでした。"
    await interaction.response.send_message(msg, ephemeral=True)


@bot.tree.command(name="list", description="このサーバーに登録されているRSSフィードを表示します")
@app_commands.guild_only()
@app_commands.check(is_admin_or_owner)
async def list_feeds(interaction: discord.Interaction):
    async with bot.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT channel_id, rss_url FROM rss_feeds WHERE guild_id = $1 ORDER BY id", interaction.guild_id
        )
    if not rows:
        await interaction.response.send_message("登録されているフィードはありません。", ephemeral=True)
        return
    text = "\n".join(f"<#{r['channel_id']}> ← {r['rss_url']}" for r in rows)
    await interaction.response.send_message(text[:1900], ephemeral=True)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN が設定されていません(.env を確認してください)")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL が設定されていません(.env を確認してください)")
    bot.run(TOKEN)

# discord bot triggering great expectations data quality reports on demand
import asyncio
import os
import sys
import discord
from discord.ext import commands
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_quality.checks import SUPPORTED_TABLES, run_quality_checks

load_dotenv()

ALLOWED_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "").strip()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


def build_embed(reports: list[dict]) -> discord.Embed:
    all_passed = all(r["success"] for r in reports)
    color = discord.Color.green() if all_passed else discord.Color.red()
    title = "Data Quality Report: ALL PASSED" if all_passed else "Data Quality Report: FAILURES DETECTED"
    embed = discord.Embed(title=title, color=color)

    for r in reports:
        icon = "PASS" if r["success"] else "FAIL"
        body = f"Rows: `{r['row_count']}` | Checks: `{r['passed']}/{r['total']}`"
        if r["failures"]:
            fail_lines = [
                f"- `{f['expectation']}` ({f['column']}): {f['detail']}"
                for f in r["failures"][:5]
            ]
            body += "\n" + "\n".join(fail_lines)
        embed.add_field(name=f"[{icon}] {r['table']}", value=body, inline=False)
    return embed


@bot.event
async def on_ready():
    print(f"discord bot ready as {bot.user}")


@bot.command(name="dq")
async def dq_command(ctx: commands.Context, table: str | None = None):
    if ALLOWED_CHANNEL_ID and str(ctx.channel.id) != ALLOWED_CHANNEL_ID:
        return

    if table and table not in SUPPORTED_TABLES:
        valid = ", ".join(SUPPORTED_TABLES)
        await ctx.send(f"Unknown table `{table}`. Available: `{valid}`")
        return

    target = [table] if table else None
    await ctx.send(f"Running Great Expectations checks on `{table or 'all gold tables'}`...")
    try:
        reports = await asyncio.to_thread(run_quality_checks, target)
        await ctx.send(embed=build_embed(reports))
    except Exception as err:
        await ctx.send(f"Data quality check error: `{err}`")


@bot.command(name="dq_help")
async def dq_help_command(ctx: commands.Context):
    tables = ", ".join(f"`{t}`" for t in SUPPORTED_TABLES)
    await ctx.send(f"Commands:\n- `!dq` — check all gold tables\n- `!dq <table>` — check one table ({tables})")


if __name__ == "__main__":
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is missing in .env")
    bot.run(token)

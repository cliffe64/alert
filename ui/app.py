"""Streamlit user interface for the alerting system."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List

import streamlit as st

from rules.config_loader import load_config
from storage import sqlite_manager


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _dashboard() -> None:
    st.header("行情仪表盘")
    config = load_config()
    cols = st.columns(2)
    with cols[0]:
        st.subheader("最新价 / 涨跌")
        rows: List[Dict[str, object]] = []
        for symbol in config.symbols:
            bar = sqlite_manager.fetch_latest_bar("bars_1m", symbol)
            if not bar:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "close": bar["close"],
                    "close_ts": _format_ts(int(bar["close_ts"])),
                }
            )
        st.dataframe(rows)
    with cols[1]:
        st.subheader("最近事件")
        events = sqlite_manager.fetch_undelivered_events(limit=20)
        table = [
            {
                "id": e["id"],
                "symbol": e["symbol"],
                "rule": e["rule"],
                "severity": e["severity"],
                "ts": _format_ts(int(e["ts"])),
            }
            for e in events
        ]
        st.dataframe(table)


def _price_alerts() -> None:
    st.header("价格提醒规则")
    rules = sqlite_manager.list_rules()
    st.dataframe(rules)
    st.caption("规则的 CRUD 可直接通过数据库或后续版本完善。")


def _volume_trend_config() -> None:
    st.header("放量 / 趋势参数")
    config = load_config()
    st.json({"volume_spike": config.volume_spike.mode.value, "trend_channel": config.trend_channel.__dict__})


def _notification_settings() -> None:
    st.header("通知设置")
    config = load_config()
    ding = config.notifiers.dingtalk
    st.subheader("钉钉")
    st.write({"enabled": ding.enabled, "webhook": ding.webhook, "secret": bool(ding.secret)})
    if st.button("测试钉钉推送"):
        st.info("请在服务端运行 alerts.router.dispatch_new_events() 触发测试")
    sound = config.notifiers.local_sound
    st.subheader("本地声音")
    st.write({"enabled": sound.enabled, "sound_file": sound.sound_file, "volume": sound.volume})


def _token_registry() -> None:
    st.header("代币注册表")
    tokens = sqlite_manager.list_tokens()
    st.dataframe(tokens)


def main() -> None:
    st.set_page_config(page_title="Alert Service", layout="wide")
    page = st.sidebar.selectbox(
        "功能模块",
        (
            "仪表盘",
            "价格提醒",
            "放量/趋势配置",
            "通知设置",
            "代币注册表",
        ),
    )
    if st.sidebar.button("刷新配置"):
        st.experimental_rerun()

    if page == "仪表盘":
        _dashboard()
    elif page == "价格提醒":
        _price_alerts()
    elif page == "放量/趋势配置":
        _volume_trend_config()
    elif page == "通知设置":
        _notification_settings()
    else:
        _token_registry()


if __name__ == "__main__":
    main()

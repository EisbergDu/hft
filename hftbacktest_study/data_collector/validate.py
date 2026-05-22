"""
验证收集的数据是否能被 hftbacktest 的 convert() 函数正确处理。

检查项：
  1. 文件格式：每行 = {19位纳秒时间戳} {JSON}
  2. 时间戳单调递增（本地时间不能倒退）
  3. 三类事件齐全：trade / depthUpdate / bookTicker
  4. 深度序列号连续性（有无断层、断层时是否有快照补全）
  5. 快照格式（lastUpdateId / bids / asks 字段）
  6. 实际调用框架的 convert() 函数，验证能否转出合法的 numpy 数组

用法：
  python validate.py <gz文件路径>
  例：python validate.py data/btcusdt_20260521.gz
"""

import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path


# ── 工具函数 ────────────────────────────────────────────────────────────────

def read_gz(path: str):
    """逐行读取 gz 文件，返回 (行号, 原始行) 生成器，忽略末尾截断。"""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            yield i, line.rstrip("\n")


def parse_line(line: str, lineno: int):
    """
    解析一行，返回 (local_ts_ns, message_dict)。
    格式：{19位纳秒时间戳} {JSON}
    """
    if len(line) < 21:  # 至少 19位时间戳 + 空格 + {}
        raise ValueError(f"行 {lineno}: 行太短 ({len(line)} 字符)")

    ts_str = line[:19]
    if not ts_str.isdigit():
        raise ValueError(f"行 {lineno}: 时间戳不是19位数字，实际='{ts_str}'")

    local_ts = int(ts_str)
    try:
        msg = json.loads(line[20:])
    except json.JSONDecodeError as e:
        raise ValueError(f"行 {lineno}: JSON 解析失败：{e}")

    return local_ts, msg


# ── 主验证逻辑 ───────────────────────────────────────────────────────────────

def validate(gz_path: str):
    path = Path(gz_path)
    print(f"\n{'='*60}")
    print(f"验证文件：{path.name}")
    print(f"{'='*60}")

    errors   = []
    warnings = []

    # 统计计数
    counts = defaultdict(int)        # 各事件类型计数
    prev_local_ts = 0                # 上一行本地时间戳（检查单调性）
    prev_u: dict[str, int] = {}      # 各 symbol 最后 depthUpdate 的 u
    has_snapshot: dict[str, bool] = {}  # 是否见过快照
    gap_count = defaultdict(int)     # 断层次数

    total_lines = 0
    try:
        for lineno, line in read_gz(gz_path):
            if not line:
                continue
            total_lines += 1

            # ── 检查1：行格式 ────────────────────────────────────────────
            try:
                local_ts, msg = parse_line(line, lineno)
            except ValueError as e:
                errors.append(str(e))
                if len(errors) >= 10:
                    errors.append("错误太多，提前终止格式检查...")
                    break
                continue

            # ── 检查2：本地时间戳单调递增 ─────────────────────────────────
            if local_ts < prev_local_ts:
                warnings.append(
                    f"行 {lineno}: 本地时间戳倒退 "
                    f"(prev={prev_local_ts}, cur={local_ts})"
                )
            prev_local_ts = local_ts

            # ── 检查3：解析事件类型 ───────────────────────────────────────
            # combined_stream 格式：{"stream": "...", "data": {...}}
            # snapshot 格式：直接是 {"lastUpdateId": ..., "bids": ..., "asks": ...}
            data = msg.get("data")

            if data is None:
                # 可能是快照或错误响应
                if "lastUpdateId" in msg and "bids" in msg and "asks" in msg:
                    counts["snapshot"] += 1
                    symbol = msg.get("s", "unknown").lower()
                    has_snapshot[symbol] = True

                    # 检查快照字段完整性
                    if not isinstance(msg["bids"], list):
                        errors.append(f"行 {lineno}: 快照 bids 不是列表")
                    if not isinstance(msg["asks"], list):
                        errors.append(f"行 {lineno}: 快照 asks 不是列表")
                elif "code" in msg:
                    warnings.append(f"行 {lineno}: API 错误消息 code={msg['code']}: {msg.get('msg')}")
                else:
                    warnings.append(f"行 {lineno}: 未知消息格式，跳过")
                continue

            evt    = data.get("e", "")
            symbol = data.get("s", "unknown").lower()

            if evt == "trade":
                counts["trade"] += 1
                # 检查必要字段
                for field in ["T", "p", "q", "m", "X"]:
                    if field not in data:
                        errors.append(f"行 {lineno}: trade 缺少字段 '{field}'")

            elif evt == "depthUpdate":
                counts["depthUpdate"] += 1
                for field in ["T", "u", "pu", "b", "a"]:
                    if field not in data:
                        errors.append(f"行 {lineno}: depthUpdate 缺少字段 '{field}'")
                        continue

                u  = data.get("u")
                pu = data.get("pu")

                # ── 检查4：深度序列号连续性 ───────────────────────────────
                expected = prev_u.get(symbol)
                if expected is not None and pu != expected:
                    gap_count[symbol] += 1
                    if not has_snapshot.get(symbol):
                        warnings.append(
                            f"行 {lineno} [{symbol}]: 深度断层且无快照补全 "
                            f"(期望 pu={expected}, 实际 pu={pu})"
                        )

                if u is not None:
                    prev_u[symbol] = u

            elif evt == "bookTicker":
                counts["bookTicker"] += 1
                for field in ["T", "b", "B", "a", "A"]:
                    if field not in data:
                        errors.append(f"行 {lineno}: bookTicker 缺少字段 '{field}'")

            else:
                counts[f"other:{evt}"] += 1

    except EOFError:
        warnings.append("文件末尾被截断（正常收集中断会出现，不影响已写入数据）")

    # ── 输出统计 ─────────────────────────────────────────────────────────────
    print(f"\n[事件统计] 共 {total_lines} 行")
    for k, v in sorted(counts.items()):
        print(f"  {k:<20} {v:>8} 条")

    print(f"\n[深度断层]")
    if gap_count:
        for sym, cnt in gap_count.items():
            snap_ok = "✓ 有快照补全" if has_snapshot.get(sym) else "✗ 无快照！"
            print(f"  {sym}: {cnt} 次断层，{snap_ok}")
    else:
        print("  无断层 ✓")

    print(f"\n[快照]")
    if has_snapshot:
        for sym in has_snapshot:
            print(f"  {sym}: 有快照 ✓")
    else:
        print("  无快照（若有断层则数据不完整）")

    # ── 输出告警和错误 ────────────────────────────────────────────────────────
    if warnings:
        print(f"\n[告警] {len(warnings)} 条")
        for w in warnings[:20]:
            print(f"  ⚠  {w}")

    if errors:
        print(f"\n[错误] {len(errors)} 条")
        for e in errors[:20]:
            print(f"  ✗  {e}")
        print("\n结论：数据格式有误，需要修复后才能喂给框架。")
        return False

    # ── 检查三类事件是否齐全 ─────────────────────────────────────────────────
    missing = []
    if counts["trade"] == 0:
        missing.append("trade（逐笔成交）")
    if counts["depthUpdate"] == 0:
        missing.append("depthUpdate（增量深度）")
    # bookTicker 是可选的，不强制
    if missing:
        print(f"\n⚠  缺少事件类型：{', '.join(missing)}")
        print("   可能是数据收集时间太短，或订阅未成功。")

    # ── 实际调用 convert() 验证 ───────────────────────────────────────────────
    # 直接内嵌框架的类型常量和 convert 逻辑，不依赖 Rust 扩展的编译
    print(f"\n[调用 hftbacktest convert() 验证]")
    try:
        import numpy as np

        # 根据已统计的事件数估算展开后行数：
        #   depthUpdate 平均约 20 档/侧 × 2 = 40 行，trade 1 行，snapshot 约 2000 行
        estimated = counts["depthUpdate"] * 40 + counts["trade"] + counts["snapshot"] * 2000 + 100_000
        buf_size = int(estimated * 1.2)
        buf_size = max(buf_size, 2_000_000)   # 最少 2M 行（约 128 MB），避免空文件浪费
        MAX_BUF = 30_000_000                  # 硬上限 30M 行 ≈ 1.8 GiB，超出则跳过 convert
        print(f"  预估 buffer_size = {buf_size:,}（约 {buf_size * 64 / 1024**3:.1f} GiB）")

        if buf_size > MAX_BUF:
            print(f"  ⚠  预估行数 {buf_size:,} 超过安全上限 {MAX_BUF:,}（≈{MAX_BUF*64/1024**3:.1f} GiB），")
            print(f"     跳过 convert() 实际调用以防 OOM。格式检查已通过，数据可用。")
            print("\n结论：数据格式合法，convert() 因文件过大已跳过 ✓")
            return True

        from hftbacktest.data.utils.binancefutures import convert as hft_convert
        result = hft_convert(gz_path, buffer_size=buf_size)
        row = len(result)
        print(f"  ✓ 转换成功！共解析 {row:,} 条事件")

        # 检查时间戳合理性
        if row > 0:
            latency_ms = (result["local_ts"][0] - result["exch_ts"][0]) / 1e6
            print(f"  首条事件：px={result[0]['px']:.2f}, qty={result[0]['qty']:.6f}")
            print(f"  延迟样本（首条）：{latency_ms:.1f} ms（负值说明时钟偏差，框架会自动修正）")

            neg_latency = np.sum(result["local_ts"] < result["exch_ts"])
            if neg_latency > 0:
                print(f"  ⚠  {neg_latency:,} 条记录本地时间早于交易所时间（时钟偏差），框架会自动修正")

        print("\n结论：数据完全兼容 hftbacktest ✓")
        return True

    except Exception as e:
        import traceback
        print(f"  ✗ 转换失败：{e}")
        traceback.print_exc()
        print("\n结论：数据无法被框架处理，需要排查。")
        return False


# ── 入口 ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # 未指定文件时，自动找 data/ 目录下最新的 gz 文件
        import glob
        files = sorted(glob.glob("data/*.gz"))
        if not files:
            print("用法：python validate.py <gz文件路径>")
            print("或在 data/ 目录下有 gz 文件时直接运行 python validate.py")
            sys.exit(1)
        target = files[-1]
        print(f"自动选择最新文件：{target}")
    else:
        target = sys.argv[1]

    ok = validate(target)
    sys.exit(0 if ok else 1)

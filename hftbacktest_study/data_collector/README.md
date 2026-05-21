# 数据收集器使用说明

收集 Binance U 本位合约实时行情，输出格式与 hftbacktest 完全兼容。

## 收集的数据

| 数据流 | 内容 |
|---|---|
| `@trade` | 逐笔成交 |
| `@bookTicker` | 最优买卖盘 |
| `@depth@0ms` | 增量深度更新（最快频率） |

当检测到深度序列号断层时，自动拉取 REST 快照补全，确保订单簿可以正确重建。

## 依赖安装

```bash
pip install websockets aiohttp
```

## 修改收集品种

编辑 `collector.py` 顶部的配置：

```python
SYMBOLS = ["BTCUSDT", "ETHUSDT"]  # 按需增减
OUTPUT_DIR = "./data"              # 数据输出目录
```

## 启动收集（screen 后台运行）

```bash
cd ~/hft/hftbacktest_study/data_collector

screen -dmS hft_collector bash -c 'python collector.py >> collector.log 2>&1'
```

## 常用管理命令

```bash
# 查看实时日志
tail -f collector.log

# 进入 screen 查看
screen -r hft_collector

# 从 screen 退出（不停止进程）
Ctrl+A 然后按 D

# 查看所有 screen
screen -ls

# 停止收集
screen -S hft_collector -X quit
```

## 输出文件

数据按天自动滚动，存放在 `data/` 目录：

```
data/
├── btcusdt_20260521.gz
├── btcusdt_20260522.gz
├── ethusdt_20260521.gz
└── ethusdt_20260522.gz
```

## 验证数据是否可用

```bash
# 验证指定文件
python validate.py data/btcusdt_20260521.gz

# 自动验证最新文件
python validate.py
```

验证通过后，数据可直接用 hftbacktest 的 `convert()` 函数处理：

```python
from hftbacktest.data.utils.binancefutures import convert

data = convert("data/btcusdt_20260521.gz")
```

# HFT 量化高频交易研究

## 克隆项目

```bash
git clone --recurse-submodules https://github.com/EisbergDu/hft.git
```

如果已经克隆但忘记带 `--recurse-submodules`，补救：

```bash
git submodule update --init
```

## 目录结构

```
hft/
├── CLAUDE.md                        # Claude Code 项目说明
└── hftbacktest_study/
    ├── 学习路线.md                   # hftbacktest 学习路线
    └── hftbacktest/                 # hftbacktest 源码（submodule）
```

#!/usr/bin/env bash
# lux 单元测试脚本（手动执行）
#
# 用法:
#   ./test.sh           运行离线单元测试（编译检查 + 下载队列 + 工具包，无需网络）
#   ./test.sh --all     运行全部测试（包含需要网络访问的 extractor/downloader 集成测试）
#   ./test.sh --race    离线单元测试 + race 检测
set -euo pipefail
cd "$(dirname "$0")"

RUN_ALL=false
RACE_FLAG=""
for arg in "$@"; do
  case "$arg" in
    --all)  RUN_ALL=true ;;
    --race) RACE_FLAG="-race" ;;
    *) echo "未知参数: $arg"; exit 2 ;;
  esac
done

echo "==> [1/4] go build ./..."
go build ./...

echo "==> [2/4] go vet ./app/ ./downloader/ ./utils/ ./parser/"
go vet ./app/ ./downloader/ ./utils/ ./parser/ || true

echo "==> [3/4] 下载队列管理器单元测试 (downloader 包)"
go test $RACE_FLAG -count=1 -v -run 'TestQueue|TestPauseController' ./downloader/

echo "==> [4/4] 离线单元测试 (utils / parser 包，跳过需要网络的 TestGetNameAndExt)"
go test $RACE_FLAG -count=1 -skip 'TestGetNameAndExt' ./utils/ ./parser/

if [ "$RUN_ALL" = true ]; then
  echo "==> 全部测试（包含需要网络的 extractor / request / downloader 集成测试）"
  go test $RACE_FLAG -count=1 ./...
fi

echo ""
echo "所有测试通过 ✅"

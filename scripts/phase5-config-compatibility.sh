#!/usr/bin/env bash
set -euo pipefail

# Phase 5 P5-D14：配置兼容性必须先显式迁移，再通过同一 SettingsRegistry 强类型校验与 AD-035 scope resolution。
# 未来版本、迁移缺口、rename 冲突、未知配置和非法值都必须明确失败，安全敏感默认值不得静默降级。

echo "[P5-D14] settings/config compatibility"
go test -count=1 ./internal/settings

echo "[P5-D14] settings/config race safety"
go test -race -count=1 ./internal/settings

echo "P5-D14 config compatibility gate: PASS"

# 规则层清零硬编码清单（阶段化）

更新时间：2026-04-01

## 一、已迁移到前端可配（本次）

1. 汇总分析科目清单
- 旧位置：`webapp/server.py` 常量 `DEFAULT_SUMMARY_ANALYSIS_ITEMS`
- 新位置：`config/rulebook.xlsx` / `summary_analysis_items`
- 前端入口：规则配置 -> 资产负债分析规则 -> 汇总分析科目

2. 汇总分析排除编码
- 旧位置：`webapp/server.py` 常量 `DEFAULT_SUMMARY_EXCLUDE_CODES`
- 新位置：`config/rulebook.xlsx` / `summary_exclude_codes`
- 前端入口：规则配置 -> 资产负债分析规则 -> 汇总排除科目

3. 分析编码重定向
- 旧位置：`webapp/server.py` 常量 `DEFAULT_ANALYSIS_CODE_REDIRECTS`
- 新位置：`config/rulebook.xlsx` / `analysis_code_redirects`
- 前端入口：规则配置 -> 资产负债分析规则 -> 分析编码重定向

说明：以上三项均保留代码默认兜底（sheet 缺失时不中断）。

## 二、仍在代码控制（待迁移）

### P1（优先，建议下一批）
1. `SUMMARY_ANALYSIS_ITEMS` 的字段约束逻辑
- 当前 `kind` 的解释与取值校验在代码中。

2. 汇总构成算法参数
- 如 Top 构成去重逻辑、重叠判定细节（文本去重规则）。

3. 资产/负债分析页部分文案兜底
- 代码里的 fallback 文案仍存在，建议迁到规则表并前端维护。

### P2（中期）
4. 明细识别策略参数
- 例如列名回退映射、明细识别优先级。

5. 重点指标（ROE/毛利率）展示策略开关
- 算法保留代码，展示策略与阈值尽量规则化。

### P3（保留代码层，不迁移）
6. 算法机制本体
- ROE Shapley 分解
- 毛利率结构/价格成本/交互分解
- 规则缺失降级与告警机制

## 三、执行原则（已对齐）
- 算法由代码控制；规则口径前端可调。
- 缺失规则不中断，输出“待补充”，写入告警清单。
- 迁移采用“先兼容后清理”策略，避免存量模板失效。

## 四、下一步建议（可直接执行）
1. 迁移汇总构成去重参数到规则表（新 sheet：`summary_composition_policy`）。
2. 迁移资产/负债 fallback 文案到规则表（减少代码内默认句式）。
3. 规则页增加“字段说明”帮助，降低误改风险。

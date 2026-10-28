# 算子分类与图标

[English](operator-presentation.md)

开发者提供 `icon`、`inputs`、`parameters`。仓库管理员在审批时指定**多个分类标签**。提交内容不能包含 `category` 或 `categories`；批准后的公开清单采用 `categories: [Climate, Conversion]`，单数 `category` 已删除。

```yaml
icon:
  path: M3 5h18v14H3zM3 10h18M9 5v14
  color: '#14866b'
inputs:
  source:
    representations: [records]
    formats: []
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

审批要求 1–16 个不重复标签，每个 1–80 字符，不包含首尾空白或控制字符。同一清单不能重复仅大小写不同的标签。管理员可选已有分类或增加新分类，无需修改注册表或重建服务。公开搜索匹配全部标签；设计器在每个所属分类中列出同一算子，从不同分类拖入时仍使用相同算子引用。内置 `read_item` 使用内置 Data sources 分类。

`icon` 仅包含 `path`、`color`。`path` 使用 SVG 24×24 坐标，以 M/m 开始，最长 2048 字符，仅允许路径命令和数字字符，可包含多个子路径。`color` 为 `#RRGGBB`，采用圆角描边、不填充。不接受完整 SVG、脚本、HTML、外部 URL 或图像文件。校验约束语法，不判断图标美观程度。

公开卡片与详情也显示管理员评定的 0–5 分可信度。原始扫描得分和发现保存在私有审核记录中。参见[仅源码提交与审核](operator-submission_zh.md)。共享校验位于 `dacp-operator-contract` 的 `dacp.operator.OperatorPresentation`。

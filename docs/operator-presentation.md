# 算子分类与图标

每个本地 `operator.yaml`、发布用 `--ports FILE` 和 HTTP 发布清单都必须包含 `category`、`icon`、`inputs` 和 `parameters`。字段缺失或非法时拒绝发布、目录读取和节点加载。没有旧字段兼容或默认分类。

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M3 14h18M9 5v14"
  color: "#14866b"
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

`category` 是作者指定的分类名称，1–80 个字符，无首尾空白和控制字符；同名分类合并，区分大小写。直接填写业务领域或用途，例如 `Climate analysis`、`Format conversion`。不需要在核心注册分类或重新编译服务器。

`icon` 只含 `path` 与 `color`。`path` 使用 SVG 的 24×24 坐标系，必须以 M/m 开始，最多 2048 字符，只允许路径指令和数值字符；可含多个子路径。`color` 必须是 `#RRGGBB`。渲染使用无填充、圆角描边。不要提交完整 SVG、脚本、HTML、外部 URL 或图片文件。作者应预览实际形状；校验限制元数据结构和字符，不验证图标的艺术效果。

仓库保留这些字段，公开列表、搜索和详情页展示分类与图标；搜索支持分类名称。设计器按声明分类展示可折叠图标网格，悬停查看详细契约。每个算子独立提供图标，同分类也可有不同图标。内置 `read_item` 同样明确声明自己的分类与图标。

发布仍采用[认证上传 API](operator-packaging.md)或管理员 CLI；`--ports` 文件中包含以上字段即可，无需新增命令行参数。完整定义还需提供 `name`、`version`、`image` 等执行设置。共享校验实现为 `dacp.operator.OperatorPresentation`，位于轻量 `dacp-operator-contract` 模块。

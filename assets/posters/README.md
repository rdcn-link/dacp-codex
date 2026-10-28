# DACP 会议海报

英文会议海报介绍自主数据节点、记录与字节流、YAML 流式作业、可复用算子、执行策略和运行监控。面向项目交流，优先介绍用户能力，不罗列底层传输技术。

- [PNG 海报](dacp-meeting-poster-en.png)：用于预览和数字分享。
- [A4 PDF](dacp-meeting-poster-en-a4.pdf)：单页 A4，保持 PNG 的宽高比，页脚仓库地址可点击。
- [生成提示词](dacp-meeting-poster-en-prompt.txt)：记录完整文案、版式和图示约束。

海报沿用 [DACP 标识](../dacp-logo.png)及紫色、青色、琥珀色三个节点。虚线表示 Union 对 A、B、C 的协调；实线表示 A → B → C 的直接数据流，最终结果从 C 经 Union 返回客户端。各节点也可以独立提供数据访问。

内容涵盖可插拔数据目录、Java / Scala、Python 和原生命令算子、变量、超时、重试、可选物化与重放，以及 DAG、健康状态和流量速率监控。物化由执行策略控制，不意味着所有作业都会保存中间结果。

PNG 使用内置 imagegen 工具基于现有海报与 Logo 更新；PDF 嵌入同一 PNG。项目仍处于 MVP 阶段，海报不声明性能数字或“数据绝不移动”等保证。

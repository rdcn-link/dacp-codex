# DACP 会议海报

英文会议海报介绍自主数据节点、bytes / Arrow records 流、Java / Scala、Python 与原生命令算子，以及 Union 跨节点工作流和运行监控。

- [PNG 海报](dacp-meeting-poster-en.png)：用于预览和数字分享。
- [A4 PDF](dacp-meeting-poster-en-a4.pdf)：单页 A4，保持 PNG 的宽高比，页脚仓库地址可点击。
- [生成提示词](dacp-meeting-poster-en-prompt.txt)：记录文案、版式和图示约束。

海报使用 [DACP 标识](../dacp-logo.png)，保留 C 形图案与紫色、青色、琥珀色三个节点。图中虚线表示 Union 对服务器 A、B、C 的协调关系；实线表示 A → B、C → B 的直接数据流。Union 是协作调度组件，各服务器可以独立提供数据服务。

PNG 由 imagegen 基于原海报和现有标识更新；PDF 嵌入同一 PNG，不额外修改图像内容。项目仍处于 MVP 阶段，海报不声明性能数字或“数据绝不移动”等保证。

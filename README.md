## 转账交易信息的数据分析
给定一组转账交易数据，计算每个账户的交易次数，每个账户的收款总金额，以及转出的接收账户数。分别显示交易次数超过5次、收款总金额超过5000、接收账户数超过两个的交易账户。

### 基本要求
- 转账交易数据至少包括转出账户ID，转入账户ID，交易金额三个字段。
- 转账交易数据随机生成，并保存在一个文本文件中。
- 数据分析程序自动提取文件中交易数据信息，进行分析与统计。
- 数据流对象以模板类形式实现。
- 数据对象支持filter、map、reduce、keyBy等基本数据分析操作。
- 除随机数生成等基本操作外，不得调用第三方库。

### 高级要求
- 模拟数据特征的分布式计算与聚合。
- 支持多种类型数据流来源的实时输入与解析。

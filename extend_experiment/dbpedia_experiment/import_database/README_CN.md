# DBpedia 导入 Neo4j

`import_data.py` 直接读取同级 `dataset/` 中的四个 `.ttl.bz2` 文件，流式解析 RDF，
通过官方 Neo4j Python Driver 批量写入。需要 Python **3.11+** 和 Neo4j **5.26+**，
支持项目记录的 **2026.06.0**，Community / Enterprise 均可，不需要 APOC 或 n10s。

## 使用

在项目根目录、项目 Python 环境中安装依赖。当前项目使用 Conda：

```bash
conda activate graph_database_denormalization
conda install -c conda-forge neo4j-python-driver pyoxigraph
```

如果使用独立的 pip/venv 环境，可安装本目录的 `requirements.txt`。

在 `import_data.py` 顶部的连接配置区填写密码和目标数据库名称：

```python
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""  # 在这里填写你的密码
NEO4J_DATABASE = "dbpedia"
```

密码留空且未设置 `NEO4J_PASSWORD` 环境变量时，程序会在运行时询问密码。
启动 Neo4j 后执行导入：

```bash
python dbpedia_experiment/import_database/import_data.py
```

指定目标数据库和批次大小：

```bash
python dbpedia_experiment/import_database/import_data.py --database dbpedia --batch-size 5000
```

程序也接受 `NEO4J_URI`、`NEO4J_USER`、`NEO4J_PASSWORD`、`NEO4J_DATABASE` 环境变量，以及
`--uri`、`--user`、`--data-dir` 参数。连接配置的优先级为：显式命令行参数 > 环境变量 > 文件顶部配置。
正常导入时，程序先连接 `system` 数据库：目标库不存在就执行
`CREATE DATABASE $name IF NOT EXISTS WAIT 60 SECONDS`，确认其状态为 `online` 后开始导入；已存在的库直接使用。
自动创建数据库需要 Neo4j Enterprise 和建库权限。Community 可导入已有数据库，例如将 `NEO4J_DATABASE` 改为 `"neo4j"`。

首次导入显示普通初始化提示：

```text
[database] dbpedia is online
[database] First import: initializing import metadata and constraints.
[import] instance-types_lang=en_specific.ttl.bz2
```

准备阶段先读取已有标签，避免查询空库中尚不存在的导入标签所产生的长通知。
程序未关闭 Neo4j 的警告或错误日志。

四个文件写入完成后直接结束，不再运行逐条 `verify`，也不再执行结束时的全图节点/关系计数。
结束提示为：

```text
Imported 67,046,342 source triples.
```

`--verify-only` 已移除。已经进入旧版 `[verify]` 阶段的进程可以停止，已提交的导入数据会保留，
无需因为移除验证而重新导入。修改脚本不会自动改变已启动进程的执行逻辑。

## 数据如何存储

| 来源 | Neo4j 表示 |
| --- | --- |
| 四个文件中的实体 URI，以及 objects 文件的客体 URI | `(:DBpediaResource {uri: 完整URI})`，URI 唯一约束 |
| `rdf:type` | 完整类型 URI 作为标签，例如 `http://dbpedia.org/ontology/Band`，同时在 `rdf_types` 字符串数组中保留该 URI |
| `rdfs:label` | 属性名为 `http://www.w3.org/2000/01/rdf-schema#label` 的字符串数组 |
| 字面值属性 | 完整谓词 URI 作为属性名，例如 `http://dbpedia.org/ontology/alias`，值为 `["Chk Chk Chk"]` |
| 资源关系 | 完整谓词 URI 作为有向关系类型，例如 `http://dbpedia.org/ontology/bandMember`；关系的 `predicate` 属性也保存该 URI |

参考 [main.cpp 的 readDBpedia()](../../GraphNormalization-main/main.cpp)，类型、属性和关系名称直接使用完整 URI。
程序中没有前缀表或缩写转换，不同命名空间的同名词汇通过完整 URI 自然区分。
例如 `http://dbpedia.org/ontology/name` 和 `http://dbpedia.org/property/name` 是两个不同属性。
在 Cypher 查询中用反引号包围完整 URI，或用 `n[$property_uri]` 动态访问属性；导入程序使用参数传递这些名称。
原始 URI 不截断、不改写；只有名称或只作为关系客体出现的资源也会保留。
`specific` 类型不展开父类，不执行推理或领域筛选。

本程序继续读取当前 dataset 中的四个文件。参考代码还读取 commons-sameAs 和 infobox 文件，
本目录没有这两类文件，因此本程序不执行相应的实体别名合并或额外属性导入。

为保持导入简单并保留原始值，**所有字面值属性都使用字符串数组，包括数字和日期**。
例如属性 `http://dbpedia.org/ontology/activeYearsStartYear` 保存为 `["1996"]`；需要计算时可在查询中使用 `toInteger()`。
程序不把原始年份转换成完整日期，不把高精度小数转换成浮点数。

每个节点另有 `rdf_literals` 字符串数组，每个元素是一条 JSON 记录：

```text
[完整谓词URI, 原始词法值, 完整datatype URI, 语言或null]
```

例如别名的记录为：

```json
["http://dbpedia.org/ontology/alias","Chk Chk Chk","http://www.w3.org/1999/02/22-rdf-syntax-ns#langString","en"]
```

相同文本在不同语言或 datatype 下的记录会分别保留。查询用属性数组按文本去重，
完整 RDF 字面值身份以 `rdf_literals` 为准。JSON 以字符串存储，因为 Neo4j 属性不能直接保存嵌套字典。
这会增加存储空间，但保留了每个值和语言、类型之间的对应关系。

## 正确性与重试

一次正常运行依次完成：

1. 计算四个压缩文件的 SHA-256，识别当前数据快照；不再进行导入前的全量 RDF `check`。
2. 创建缺失的目标数据库并确认其在线，再创建 URI 唯一约束和一个 `:DBpediaImport` 导入记录。边解析边检查每种文件的谓词/客体类型，批量 `MERGE` 节点、属性和关系，并统计源三元组数。
3. 确认压缩源文件 SHA-256 未改变，标记写入完成并报告已处理的源三元组数。
   此步骤不重新解析 RDF，也不逐条读取 Neo4j 核对。

解析失败或批次写入失败时以非零退出码结束，不跳过错误记录。空白节点等超出这四类数据约定的结构会明确报错。
实测原始 labels 文件存在含 U+FFF9 等非标准字符的 IRI，因此解析使用 PyOxigraph 的 `lenient=True`。
这些标识符按原文保存，不修正或删除；程序仍检查 RDF 结构、文件角色和客体类型。
正常运行只解析每个 RDF 文件一遍，内存主要由批次大小决定。
空文件或错误记录在导入过程中发现时会停止，之前已提交的批次会保留。

同一份文件重复运行不会重复添加节点、关系或数组元素。中断后可重新运行同一命令，
会重新扫描文件并补齐数据；已提交批次不会整体回滚。
RDF 的完全重复三元组按集合语义合并，因此源记录数可能大于存储的不同 RDF 事实数。

文件校验值或映射版本不同时会拒绝写入已有 DBpedia 数据，防止混入另一份快照。
当前 `model_version` 为 **2**，采用完整 URI 名称。如果此前用带 `dbo__` 等前缀的版本导入过数据，
需要在新的空数据库或实例中重新导入；程序会拒绝混用两种名称，不会自动修改旧图。
程序不删除原有数据；使用 `DBpediaResource` 标签隔离此导入流程。
请让本程序独占该 DBpedia 数据的写入，并保持源文件不变。
保留 URI 唯一约束、`MERGE` 去重、RDF 结构检查、每批处理数量检查及来源文件匹配保护。
这些检查不等价于重新读取数据库核对所有源事实；逐条核对现已移除。

导入记录的 `complete=true` 表示所有文件写入成功，`verified=false` 表示未执行导入后的 RDF 核对。
只有所有批次和来源文件检查成功后才设置 `complete=true`；失败时保留未完成状态。
重试开始时清除旧的完成时间和源计数。FD 搜索程序分开显示写入完成和验证状态。

## 查询示例

```cypher
MATCH (band:DBpediaResource:`http://dbpedia.org/ontology/Band` {
  uri: 'http://dbpedia.org/resource/!!!'
})-[r:`http://dbpedia.org/ontology/bandMember`]->(member:DBpediaResource)
RETURN band.`http://www.w3.org/2000/01/rdf-schema#label` AS band_name,
       band.`http://dbpedia.org/ontology/activeYearsStartYear` AS start_year,
       type(r), member.uri,
       member.`http://www.w3.org/2000/01/rdf-schema#label` AS member_name;
```

## 查找类内函数依赖

运行独立目录中的 `find_functional_dependency.py`。它不导入或依赖 `import_data.py`，连接地址、用户名、
密码和数据库名在查找程序顶部单独配置；也支持 `NEO4J_*` 环境变量和命令行参数。
密码留空时会在运行时询问。程序只执行读取查询，不修改 Neo4j。

```python
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "dbpedia1"
```

查找程序不会读取 `import_database/` 中的任何连接设置。配置优先级是：命令行的
`--uri`、`--user`、`--database` > 对应的 `NEO4J_*` 环境变量 > 上述文件顶部配置；
密码使用 `NEO4J_PASSWORD` 环境变量或文件顶部配置，两者都为空时才交互询问。

```bash
python dbpedia_experiment/find_functional_dependency/find_functional_dependency.py
```

默认从小类开始，遍历数据库中**所有显式 RDF 类**，把同类节点视为表中的行、把节点上的 RDF
字面量属性视为列，同时搜索：

- 单个决定字段、单个被决定字段：`X → Y`。
- 单个决定字段、多个被决定字段：`X → {Y1, Y2, ..., Yn}`，默认 `N >= 2`。

这里的 `1 → N` 指一个字段决定 N 个字段，并不是图中的一个节点可以连接多个节点。
例如 `shippingAddress → {city, country}` 表示收货地址相同的节点，其 city 和 country 组成的完整值元组也相同。
不再以 10 条停止、不再每类只保留 2 条，也不再跳过 10 万实例以上的大类。
其他命名空间的类（例如 `owl:Thing`）也包括在内，内部标签 `DBpediaResource`、`DBpediaImport` 除外。
X、Y 都只能是当前节点上的 RDF 字面量属性。所有出边及其目标 URI 都不参与搜索；名称、节点 URI、
RDF 类型、`rdf_literals` 聚合存储和导入元数据也不作为候选属性。
程序从 `rdf_literals` 读取每个实际属性的原始值，是为了保留 datatype 和 language；报告中的属性 URI
仍对应节点 Properties 面板里的完整 URI 属性键。
只使用导入的显式类标签，不自动包含所有子类实例。没有类标签的节点不属于类内搜索的范围。
这仍不是任意形式 FD 的穷举：暂不搜索 `(X1, X2) → Y` 等组合字段依赖。

例如在 `Person` 类中，`birthDate → birthYear` 表示：两个节点的 `birthDate` 相同时，
其 `birthYear` 也必须相同。这种节点内部的重复属性组合才是本程序提供的进一步规范化候选。

判定条件与默认门槛：

- **读取整个类**。单 RHS 要求 X、Y 都恰好有一个 RDF 字面量；多 RHS 要求 X 和所有 Y 同时为单值。
  不抽样，也不使用 `LIMIT`。
- 同一个 X 只要出现两个不同 Y，就拒绝该候选；不同 X 可以对应同一个 Y。
- 至少 50 个符合条件的实例，覆盖该类至少 50%，至少 3 个 X 值分别在两个以上实例中重复。
- 排除 X 全部唯一、Y 恒定、两个字段值完全相同的情况；`X → Y` 与 `Y → X` 分别验证，均成立就都保留。
- 缺失和多值实例不参与该字段对的 FD 判定，但分别计数并显示覆盖率。
  因此 `complete_single_valued_subset` **只表示完整单值子集中的 FD**，不能宣称整个类成立；
  `whole_class` 才表示所有类实例均符合条件。
- 属性值按原始文本、datatype、language 比较，避免把不同 RDF 字面量误当成相同值。
- 多 RHS 候选先来自同一个 X 的多条合格 `X → Y`，再在所有字段的**共同参与节点**上重新验证支持数、
  覆盖率、重复 X 分组和完整 Y 元组。报告保留最大的合格 RHS 集合；如果加入稀疏字段导致不合格，
  仍会继续检查较小的字段组合。
- 每个列出的 Y 在共同参与节点中都必须至少有两个值，而且不能只是重复 X。当前做法只组合已经分别合格的
  单 RHS 依赖，因此是一种保守搜索：不会报告那种只有在排除“缺少另一个 Y”的节点后才偶然成立的多 RHS 组合。

输出保存在脚本同目录：

- `functional_dependencies_all.md`：当前进度、结果表、完整字段 URI、覆盖率和真实节点示例。
- `functional_dependencies_all.json`：`dependencies` 保存单 RHS，`multi_dependencies` 保存 1→N；
  同时保存类清单、已完成、跳过、失败和待处理的类。

每完成一个类就原子更新文件。大于 `MEMORY_CLASS_SIZE`（默认 100000）的类会流式写入临时 SQLite，
在磁盘上分组验证，完成后删除临时数据；这个值是处理方式的分界，**不是搜索上限**。
`--temp-dir` 可以指定临时磁盘位置；默认使用系统临时目录。无需另外安装 SQLite 服务或 Python 包。
`--timeout 0` 默认不为完整类读取设置事务超时，可按需要改为秒数。

中断后，使用相同搜索条件续跑：

```bash
python dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --resume
```

断点以整个类为单位：中断的当前类重新读取，完成的类不会重复计算。
恢复时核对数据库标识、导入状态/manifest、类清单及节点数、搜索参数；不匹配时要求重新开始。
这些检查不能检测所有内容修改，**续跑仍假设期间没有修改图数据**。
如果导入或修改了图，运行不带 `--resume` 的命令重新搜索。
个别类读取失败会记录错误并继续其他类，最终状态为 `incomplete`，退出码为 2；失败类可续跑重试。
Ctrl+C 保存已完成的类并以 130 退出。`running`、`interrupted`、`limited` 都不是全库搜索完成。

只查指定类，或要求 100% 类实例覆盖：

```bash
python dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --classes Archaea Plant PopulatedPlace
python dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --min-coverage 1
```

`--count`、`--max-class-size`、`--max-per-class` 默认均为 **0（不限制）**。
需要短跑时可以显式设置，例如 `--count 10 --output /tmp/dbpedia_fd_preview.json`；报告会记录数量限制。
节点数少于 `--min-support` 的类不可能通过支持数门槛，直接记录为 `below_min_support`。
没有找到依赖本身不是程序错误；全部选定范围处理完且无失败，就返回成功。
其他阈值可用 `--help` 查看，常用默认值也在文件顶部。
`--min-dependents` 控制 1→N 的最少 RHS 字段数，默认是 2。

如果还想扩大字段候选范围，可降低支持数和覆盖率，但需要更谨慎地区分偶然规律与可用于规范化的语义依赖：

```bash
python dbpedia_experiment/find_functional_dependency/find_functional_dependency.py --min-support 10 --min-coverage 0.1 --output /tmp/dbpedia_fd_broader.json
```

这些结果是**当前数据上的依赖**，不自动构成永久业务规则。例如“出生日期 → 最高排名”
即使在某个小类中零冲突，也可能只是相同运动员出现在多年的排名记录中，不能推断生日决定排名。
用于规范化实验前，应结合输出实例判断其语义。
程序分别记录导入的 `complete` 和 `verified` 状态，但不会阻止其他程序写入；没有保证整个搜索处于同一隔离快照。
若导入仍在写入，应在写入完成后重新搜索。新版导入器不执行逐条验证，因此正常写入完成后 `verified` 仍为 `false`。
旧版缺少 `verified` 字段时按旧版语义从 `complete` 推断；旧版 `complete=false` 不能判断其是否已经写完、正在核对。

2026-09-11 已在当前 `dbpedia1` 数据库完成只搜索节点字面量属性的 v5 全库运行，耗时约 3 分 42 秒。
程序盘点出 439 个显式类、7,707,325 次类成员关系（不是不同节点数）；395 个类被完整读取和判定，
44 个因节点数少于 50 排除，没有失败或待处理的类。13 个大类使用 SQLite，最大的
CareerStation 类共有 1,716,420 个实例，也已全部读取。

结果包含 **7 条单 RHS 依赖和 1 组 1→N 依赖**，详见
[`functional_dependencies_all.md`](../find_functional_dependency/functional_dependencies_all.md) 和
[`functional_dependencies_all.json`](../find_functional_dependency/functional_dependencies_all.json)：

- `SnookerWorldRanking.birthDate → activeYearsStartYear`
- `SnookerWorldRanking.birthDate → highestRank`
- `PopulatedPlace.areaCode → utcOffset`
- `Philosopher.birthDate → birthYear`
- `Actor.birthDate → birthYear`
- `Drug.fdaUniiCode → chEMBL`
- `Person.birthDate → birthYear`

唯一的多 RHS 结果是
`SnookerWorldRanking.birthDate → {activeYearsStartYear, highestRank}`。
旧结果中的 `Archaea.family/order` 来自 RDF 出边，不是节点属性，因此在此次节点内部 FD 搜索中被正确排除。
对 SnookerWorldRanking 和 Archaea 两个真实类分别强制走内存与 SQLite 两种路径复查，结果、统计和示例完全一致；
3 个离线测试也通过，覆盖关系排除、重复字面量、多值属性以及 datatype/language 区分。
这些依赖是分解模式的候选依据；是否形成有意义的 3NF/BCNF 分解，还需确认决定因素的业务语义、最小依赖集及分解后的无损连接。

此次整库运行开始和结束时，导入记录均为 `complete=true`、`verified=false`：四个文件已经写入完成，
但按新版导入流程没有执行耗时的导入后逐条 RDF 核对。FD 搜索本身已完整结束，不能替代该导入验证。

实现参考 [Neo4j 只读会话、查询超时及标识符处理](https://neo4j.com/docs/python-manual/current/query-advanced/)
和 [RDF 字面值身份](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)。

## 参考文档

- [源数据说明](../dataset/README_CN.md)：四个文件的职责和样例。
- [GraphNormalization main.cpp](../../GraphNormalization-main/main.cpp)：`readDBpedia()` 保留完整 URI，分别作为资源标识、类型、属性名及边标签。
- [PyOxigraph RDF parsing](https://pyoxigraph.readthedocs.io/en/stable/io.html)：从压缩流解析 Turtle，迭代返回 RDF 记录。
- [W3C RDF 1.1 Literals](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)：字面值的词法值、datatype 和语言信息。
- [Neo4j Python Driver 性能建议](https://neo4j.com/docs/python-manual/current/performance/#batch-data-creation)：`UNWIND` 分批写入。
- [Neo4j MERGE](https://neo4j.com/docs/cypher-manual/current/clauses/merge/)：动态关系类型和去重。
- [Neo4j COUNT 子查询](https://neo4j.com/docs/cypher-manual/current/subqueries/count/)：逐条核对匹配数量。
- [Neo4j 创建数据库](https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/create-databases/)：通过 `system` 数据库执行 `CREATE DATABASE ... IF NOT EXISTS ... WAIT`。

## 历史测试记录

以下包含移除 `verify` 之前的测试记录；当前程序不再提供 `--verify-only` 或自动逐条核对。

2026-09-10 使用项目 Python 3.13.14、PyOxigraph 0.5.6 和 Neo4j Driver 6.2.0，
完整检查了当前四个压缩文件：

| 文件 | 源三元组数（含重复记录） |
| --- | ---: |
| instance-types | 7,707,325 |
| labels | 16,904,466 |
| mappingbased-literals | 19,643,380 |
| mappingbased-objects | 22,791,171 |
| 合计 | **67,046,342** |

另在临时 Neo4j Enterprise 2026.06.0 实例中通过了集成测试，检查跨批次多值、重复导入、
高精度数值原文、转义字符、缺少名称/类型的资源，以及缺失数据、重复关系、错误谓词和不同快照的拒绝。
这验证了程序流程；正式数据库的全量写入与逐条核对由实际执行导入命令完成。

完整 URI 命名版本另通过了 3 项临时测试，覆盖不同命名空间同名词汇、完整 URI 的 Cypher 查询、
重复导入、数据核对及旧模型拒绝。还从四个实际文件中各抽取 200 条记录，
在临时 Neo4j 中成功导入并核对 **800 条三元组、562 个资源节点和 200 条关系**。

移除导入前 `check` 后，上述 3 项测试再次通过，并确认正常导入每个源文件只解析两遍，
`--verify-only` 每个源文件只解析一遍，均不再出现 `[check]` 阶段。

加入自动建库后，4 项测试通过，覆盖首次创建并导入、重复运行保留数据库及已有数据、
`--verify-only` 不建库且不修改导入记录，以及目标库离线时停止导入。测试仅使用临时实例中的数据库。

移除 `verify` 后新增 7 项临时测试通过：单次解析四个压缩文件、正常写入、批次错误、RDF 错误、
来源变化、重试状态及移除的 CLI 参数；确认不执行验证查询或结束时的全图计数。
另有 6 项测试确认 FD 报告区分写入完成与验证状态，并兼容旧版状态含义。
测试使用真实 RDF 解析与模拟 Neo4j 连接，没有重新导入正式数据。

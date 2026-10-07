# DBpedia 数据集

本目录存放四个 DBpedia 英语版知识图谱数据文件。文件名中的 `lang=en` 表示英语版本，`.ttl` 表示 RDF Turtle 格式，`.bz2` 表示使用 bzip2 压缩。数据以“主体—属性或关系—客体”的三元组形式组织。

| 文件 | 内容 | 主要用途 |
| --- | --- | --- |
| `instance-types_lang=en_specific.ttl.bz2` | 实体的直接类型，通过 `rdf:type` 表示，例如某个实体属于 `Band`（乐队）或 `Album`（专辑）。`specific` 版本不展开完整的上位类型继承链。 | 筛选实体类别，构建节点类型。 |
| `labels_lang=en.ttl.bz2` | 实体的英文名称，通过 `rdfs:label` 表示；这里的 label 是名称，不是分类标签。 | 展示名称、文本检索和文本特征构建。 |
| `mappingbased-literals_lang=en.ttl.bz2` | 实体的字面值属性，例如别名、年份、日期和数字。 | 构建节点属性和 embedding 输入特征。 |
| `mappingbased-objects_lang=en.ttl.bz2` | 实体指向其他资源的关系，例如乐队与成员、音乐风格之间的关系。 | 构建图的边，提供图结构信息。 |

`mappingbased` 表示依据 DBpedia 映射规则从 Wikipedia 信息框提取并统一表达的数据。`literals` 的客体是具体值，`objects` 的客体是资源标识；四个文件通过相同的实体 URI 关联。

## 原始数据样例

以下分别截取每个文件解压后的前两行，保留原始 URI。这些样例每行是一条三元组，结构为：

```text
<主体 URI> <属性或关系 URI> 客体 .
```

客体可以是资源 URI 或字面值。其中，URI 用 `<...>` 包围；字面值使用引号，可以附带语言或数据类型标记；末尾的 `.` 表示这条三元组结束。

### 1. 实体类型：`instance-types_lang=en_specific.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Band> .
<http://dbpedia.org/resource/!!!_(album)> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Album> .
```

结构是“实体 → 类型关系 `rdf:type` → 类”。第一行说明 `!!!` 是乐队，第二行说明 `!!!_(album)` 是专辑。

### 2. 实体名称：`labels_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!!!!!> <http://www.w3.org/2000/01/rdf-schema#label> "!!!!!!!"@en .
<http://dbpedia.org/resource/!!!> <http://www.w3.org/2000/01/rdf-schema#label> "!!!"@en .
```

结构是“实体 → 名称属性 `rdfs:label` → 名称文本”。`@en` 表示该文本的语言是英语。

### 3. 属性值：`mappingbased-literals_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/activeYearsStartYear> "1996"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/alias> "Chk Chk Chk"@en .
```

结构是“实体 → 属性 → 具体值”。第一行表示开始活动年份为 1996，`^^` 后的 `gYear` 数据类型标识说明该值是年份；第二行表示英文别名为 `Chk Chk Chk`。

### 4. 资源关系：`mappingbased-objects_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/bandMember> <http://dbpedia.org/resource/Nic_Offer> .
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/formerBandMember> <http://dbpedia.org/resource/Jerry_Fuchs> .
```

结构是“实体 → 关系 → 另一个资源”。两行分别将乐队 `!!!` 连接到成员 `Nic_Offer` 和前成员 `Jerry_Fuchs`；客体是资源 URI，而不是名称字符串。

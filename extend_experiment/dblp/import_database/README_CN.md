# DBLP 导入 Neo4j

`dblp-2024-03-01.nt.gz` 是 N-Triples RDF 文件。脚本直接流式读取 gzip，每 5000 条批量写入
Neo4j，无需解压，也无需 APOC 或 n10s。

这个版本只建立本项目需要的论文—作者属性图：

- DBLP URI 变成 `DBLPResource` 节点。
- `rdf:type` 变成 `DBLP_Publication`、`DBLP_Person` 等标签。
- `authoredBy` 变成 `AUTHORED_BY` 关系。
- title、year、name、venue 等 literal 或 URI 变成节点属性。
- 标识符和作者签名使用的 blank node 被跳过。

因此这个模型保留论文和作者之间的连接，但不保留 blank node 中的作者顺序，也不保留 literal 的
datatype/language；这与当前仓库的论文—作者实验范围一致。

当前项目环境已经安装 `neo4j` 和 `pyoxigraph`。运行：

```bash
conda activate graph_database_denormalization
python dblp/import_database/import_data.py
```

密码已脱敏；请通过 `NEO4J_PASSWORD` 环境变量提供密码，或按运行时提示输入。

默认连接 `bolt://localhost:7687` 并创建或使用 `dblp` 数据库。也可以指定参数：

```bash
python dblp/import_database/import_data.py \
  --database dblp \
  --batch-size 5000
```

先导入前 10 万条检查结果：

```bash
python dblp/import_database/import_data.py \
  --database dblp-preview \
  --limit 100000
```

脚本使用唯一约束和 `MERGE`，重复运行不会创建重复节点、属性值或作者关系。中断后重新运行会从
gzip 开头扫描。不要同时运行两个导入进程，也不要把不同日期的 DBLP 快照写入同一个数据库。

示例查询：

```cypher
MATCH (paper:DBLP_Publication)-[:AUTHORED_BY]->(author:DBLP_Person)
RETURN head(paper.`https://dblp.org/rdf/schema#title`) AS title,
       head(author.`https://dblp.org/rdf/schema#primaryCreatorName`) AS author
LIMIT 20;
```

该文件估计包含约 4 亿条三元组，因此完整导入本身仍会耗时较长。使用当前脚本实测前 1000 万条：
checkpoint 后 Neo4j store 为 346 MiB，包含 474,627 个节点和 614,573 条关系。按全量分布留出
误差后，最终数据库预计约 15–25 GiB；加上 transaction log 和导入余量，建议至少预留 30 GiB，
稳妥起见预留 40 GiB。

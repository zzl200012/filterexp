### Primary Key Deduplication via ZoneMap + Binary Fuse Filter

#### Overview

每个 block 包含一个 zonemap 和一个 filter，每个 segment 包含若干个 block 以及一个内部全局的 zonemap。当查询某个 key 时，依次遍历 segment。对于每个 segment，首先查询全局 zonemap，如果不在范围内则直接进入下一个 segment。然后使用二分查找搜索内部 block 的 zonemap，如果找到匹配的 block range 则返回该 block 对应 filter 的查询结果，否则将返回 false。

下面的实验采用 mo 的默认配置，即一个 block 16 万行，一个 segment 40 个block，故一个 segment 640 万行。调整 segment 的数目，分别进行如下测试：

1. 原始数据有序，查询数据有序，查询 N 次求出每次查询平均耗时
2. 原始数据有序，查询数据无序，查询 N 次求出每次查询平均耗时
3. 原始数据无序，查询数据有序，查询 N 次求出每次查询平均耗时
4. 原始数据无序，查询数据无序，查询 N 次求出每次查询平均耗时

#### Result

最后一行总行数64000000，但 segment 数目增加至 20，结果和 128000000 行、20 个 segment 时差不多，主要作为对照组。 

| Total Rows  | Segment Num | Block Num per Segment | Time (Sorted/Sorted) | Time (Sorted/Unsorted) | Time (Unsorted/Sorted) | Time (Unsorted/Unsorted) |
| ----------- | ----------- | --------------------- | -------------------- | ---------------------- | ---------------------- | ------------------------ |
| 64,000,000  | 10          | 40                    | 28 ns/op             | 118 ns/op              | 101 ns/op              | 360 ns/op                |
| 128,000,000 | 20          | 40                    | 43 ns/op             | 160 ns/op              | 169 ns/op              | 619 ns/op                |
| 192,000,000 | 30          | 40                    | 57 ns/op             | 185 ns/op              | 248 ns/op              | 906 ns/op                |
| 64,000,000  | 20          | 40                    | 40 ns/op             | 153 ns/op              | 162 ns/op              | 592 ns/op                |

1. 原数据、查询数据均无序时，耗时和 segment 数目呈线性关系，符合预期
2. 原数据有序时，每个 key 只会命中一个 segment，因此耗时显著小于无序的情况
3. 查询数据 layout 是否有序导致耗时差距的原因，暂时未知

代码：https://github.com/zzl200012/filterexp/blob/main/final_test.go#L11
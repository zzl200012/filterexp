## Primary Key Deduplication via Zone Map and Binary Fuse Filter

### Motivation

1. 仅点查去重，不需要 range scan
2. 允许假阳
3. 空间尽可能小，甚至放进内存
4. 粒度最好收缩到 segment 甚至 block，方便管理

### Overview

初步决定将 filter 作为排重的主要结构，相比 bloom filter、cuckoo filter 等传统 filter，发现 [binary fuse filter]([FastFilter/xorfilter: Go library implementing binary fuse and xor filters (github.com)](https://github.com/FastFilter/xorfilter)) 对于静态数据拥有更高的性能和更小的空间占用，而我们的 filter 都与 segment/block 绑定，并不会更新，所以选择了它。在 filter 的基础上，想了两套方案：

1. 纯内存方案。每个 segment 生成一个 filter，同时带有一个 segment 级的 zonemap。排重时依次遍历各个 segment，如果 zonemap 命中则查询 filter，查到阳性再去读出实际数据。查完所有 segment 仍然没出现阳性，或者查到阳性的都发现是假阳，则返回不存在。filter 和 zonemap 均常驻内存。
2. 磁盘方案。每个 segment 分为若干个 block，为每个 block 分别生成 filter 和 zonemap（或者树形索引）。排重时同样依次遍历 segment，在每个 segment 中通过二分搜索 zonemap 找到命中的区间，再搜索对应 block 的 filter，查到阳性再读入实际数据。如果所有区间都不命中或者查到假阳则进入下一个 segment，所有 segment 都没查到或者只查到假阳则返回不存在。与内存方案区别在于对一个大 filter 进行了分区，同时每个分区配备一个 zone map 加速定位，这样 filter 可以做到按需读入。

### Performance

#### 实验一：Build 耗时

测试不同数据规模下 filter 的 build 耗时

| Rows        | Time   |
| ----------- | ------ |
| 160,000     | 4 ms   |
| 500,000     | 15 ms  |
| 1,600,000   | 51 ms  |
| 5,000,000   | 0.17 s |
| 10,000,000  | 0.36 s |
| 50,000,000  | 2 s    |
| 100,000,000 | 5 s    |

因为是针对每个 segment 建，而 segment 又是异步生成，所以这部分影响不大，本身耗时也比较短。

#### 实验二：空间占用

测试不同数据规模下 filter 空间占用

| Rows        | Space     |
| ----------- | --------- |
| 160,000     | 0.18 MB   |
| 500,000     | 0.55 MB   |
| 1,600,000   | 1.72 MB   |
| 5,000,000   | 5.37 MB   |
| 10,000,000  | 10.75 MB  |
| 50,000,000  | 53.69 MB  |
| 100,000,000 | 107.37 MB |

基本上和行数呈线性关系，和 filter 数目无关，平均每个 key 占 9 个 bit。

#### 实验三：实际性能

以下实验默认一个 block 16万行，一个 segment 40 个 block，即 640 万行。（mo 默认配置）

##### Filter per segment 

将 N 个不重复的 uint64 类型数据分为若干个 segment，为每个 segment 生成一个 filter 和 zonemap，再分别用这 N 个数据查询，记录平均耗时。原始数据和查询数据 layout 可能为有序/无序。

实验代码：https://github.com/zzl200012/filterexp/blob/main/memory_test.go#L10

| Rows        | Segment Num | Time (Sorted/Sorted) | Time (Sorted/Unsorted) | Time (Unsorted/Sorted) | Time (Unsorted/Unsorted) |
| ----------- | ----------- | -------------------- | ---------------------- | ---------------------- | ------------------------ |
| 64,000,000  | 10          | 11 ns/op             | 49 ns/op               | 235 ns/op              | 233 ns/op                |
| 128,000,000 | 20          | 18 ns/op             | 71 ns/op               | 431 ns/op              | 426 ns/op                |
| 192,000,000 | 30          | 30 ns/op             | 97 ns/op               | 600 ns/op              | 612 ns/op                |
| 64,000,000  | 20          | 15 ns/op             | 66 ns/op               | 388 ns/op              | 394 ns/op                |

其中 s/s 和 us/us 分别对应最优和最坏情况，可以发现原数据无序时单次操作的耗时基本是和 filter 数目（segment 数目）呈线性关系。而原数据有序时，由于 segment 之间没有 overlap，因此每个 key 只会命中一个 segment，故而耗时显著小于无序情况。又由第四组实验可知，数据规模的影响相对要更小，所以后面的 block 方案并不会因为 filter 粒度减小而使单次 filter 操作耗时变短很多，主要还是为了按需加载，以及通过更细粒度的 zone map 尽可能过滤掉不存在的 key。

##### Filter per block

| Total Rows  | Segment Num | Block Num per Segment | Time (Sorted/Sorted) | Time (Sorted/Unsorted) | Time (Unsorted/Sorted) | Time (Unsorted/Unsorted) |
| ----------- | ----------- | --------------------- | -------------------- | ---------------------- | ---------------------- | ------------------------ |
| 64,000,000  | 10          | 40                    | 28 ns/op             | 118 ns/op              | 101 ns/op              | 360 ns/op                |
| 128,000,000 | 20          | 40                    | 43 ns/op             | 160 ns/op              | 169 ns/op              | 619 ns/op                |
| 192,000,000 | 30          | 40                    | 57 ns/op             | 185 ns/op              | 248 ns/op              | 906 ns/op                |
| 64,000,000  | 20          | 40                    | 40 ns/op             | 153 ns/op              | 162 ns/op              | 592 ns/op                |

原数据和查询数据均无序时，耗时同样和 segment 数目呈线性关系。当原数据有序时优势明显，同样符合预期。相比内存方案，在原数据有序时耗时整体变长，主要是多了一道 zone map 的流程。而数据无序时，相比纯内存方案对查询数据 layout 有明显区分度，查询数据有序时性能比无序要高三倍多，具体原因可能要看 filter 的实现，猜测和 cache miss 有关。

##### Real World

实际上，前面的实验有一个重要前提：所有查询数据都存在。并且根据观察，无论纯内存还是磁盘方案，在原数据 layout 随机的情况下实际访问 filter 的次数是原始 key 数目的五倍多，换句话说 zone map 是没有发挥其原本过滤效果的。而在实际场景中，大多数情况下查询的 key 都是不存在的。基于此我们改变查询数据，令所有 key 都不存在，经测试在不同数据规模、filter 数目下单次操作耗时在 10～30 ns，基本上就是实际场景下 zone map 生效后比较理想的效果。

### Conclusion

从结果来看，zone map + filter 这一套设计还是比较符合我们的要求的，并且实际场景下大概率会有更好的效果。
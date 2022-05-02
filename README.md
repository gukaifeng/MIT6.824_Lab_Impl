# MIT6.824 Lab 1,2,3,4 Impl

>注：所有测试均已通过 `-race` 资源竞争检测。

## Lab1

```shell
[gukaifeng@iZ8vbf7xcuoq7ug1e7hjk5Z main]$ sh ./test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

除了上面的这些外，你可能还会看到很多下面这样的输出，忽略就好。
```shell
2022/05/02 19:22:05 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```



## Lab2

```shell
[gukaifeng@iZ8vbf7xcuoq7ug1e7hjk5Z raft]$ time go test
Test (2A): initial election ...
  ... Passed --   3.0  3   56   15274    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  118   18516    0
Test (2B): basic agreement ...
  ... Passed --   0.5  3   14    3766    3
Test (2B): RPC byte count ...
  ... Passed --   1.3  3   46  113154   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   5.6  3  149   36218    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  269   48910    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.5  3   20    5532    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   2.1  3   70   14693    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  22.5  5 2669  500322  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   52   14758   12
Test (2C): basic persistence ...
  ... Passed --   3.6  3  100   22990    6
Test (2C): more persistence ...
  ... Passed --  13.3  5  992  168247   17
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.4  3   41    9578    4
Test (2C): Figure 8 ...
  ... Passed --  35.8  5 2085  397433   83
Test (2C): unreliable agreement ...
  ... Passed --   5.2  5  738  228682  252
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  32.3  5 2716  350697   29
Test (2C): churn ...
  ... Passed --  16.3  5 1206  346655  316
Test (2C): unreliable churn ...
  ... Passed --  16.3  5 1344  377140  217
PASS
ok  	_/home/gukaifeng/projects/6.824/src/raft	169.711s

real	2m49.930s
user	0m3.908s
sys	0m0.936s
```

## Lab3

```shell
```

## Lab4

```shell
```
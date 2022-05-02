# MIT6.824 Lab 1,2,3,4 Impl

>注：所有测试均已通过 `-race` 资源竞争检测。

## Lab1

```
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
```
2022/05/02 19:22:05 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```



## Lab2

```
[gukaifeng@iZ8vbf7xcuoq7ug1e7hjk5Z raft]$ time go test
Test (2A): initial election ...
  ... Passed --   3.1  3  110   30232    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  128   24809    0
Test (2B): basic agreement ...
  ... Passed --   0.6  3   14    3794    3
Test (2B): RPC byte count ...
  ... Passed --   0.9  3   46  113246   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   5.4  3  209   55562    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  350   77663    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   24    6818    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   2.3  3   99   22613    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  12.8  5 1334  632664  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   98   27906   12
Test (2C): basic persistence ...
  ... Passed --   3.5  3  128   30655    6
Test (2C): more persistence ...
  ... Passed --  11.0  5  798  153220   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.6  3   47   11317    4
Test (2C): Figure 8 ...
  ... Passed --  29.1  5 1482  294102   53
Test (2C): unreliable agreement ...
  ... Passed --   3.0  5  708  346841  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  31.5  5 1750  440162   73
Test (2C): churn ...
  ... Passed --  16.2  5 1452 1261721  662
Test (2C): unreliable churn ...
  ... Passed --  16.1  5 1965  891346  533
PASS
ok  	_/home/gukaifeng/projects/6.824/src/raft	147.686s

real	2m27.916s
user	0m4.425s
sys	0m0.900s
```

## Lab3

```shell
```

## Lab4

```shell
```
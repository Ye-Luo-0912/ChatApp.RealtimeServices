using Xunit;

// 外部基础设施模式（CHATAPP_TEST_POSTGRES / CHATAPP_TEST_GARNET）下，全部测试类共享同一个
// Postgres 实例；迁移 runner 的固定 advisory lock 等待上限 ~6s，类间并行必然争抢超时，
// 因此禁用类间并行（串行后每类独占迁移锁）。
[assembly: CollectionBehavior(DisableTestParallelization = true)]

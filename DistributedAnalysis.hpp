#ifndef DISTRIBUTED_ANALYSIS_H
#define DISTRIBUTED_ANALYSIS_H

#include "DataStream.hpp"
#include "TransactionStatis.hpp"

// 模拟数据分布式计算与聚合的框架
template <typename T>
class DistributedAnalysis
{
public:
    DistributedAnalysis(std::vector<DataStream<T>> &dataBlocks) : dataBlocks_(dataBlocks) {}

    // 在每个节点上执行数据分析操作
    std::vector<TransactionStats> analyzeOnNode()
    {
        std::vector<TransactionStats> results;
        for (const auto &dataBlock : dataBlocks_)
        {
            TransactionStats result = dataBlock.reduce();
            results.push_back(result);
        }
        return results;
    }

    // 聚合节点上的结果
    TransactionStats aggregateResults(const std::vector<TransactionStats> &results)
    {
        TransactionStats aggregatedResult;

        for (const auto &result : results)
        {
            // 对各个数据块的结果进行累加
            for (const auto &entry : result.transactionCounts)
            {
                aggregatedResult.transactionCounts[entry.first] += entry.second;
            }

            for (const auto &entry : result.totalReceivedAmounts)
            {
                aggregatedResult.totalReceivedAmounts[entry.first] += entry.second;
            }

            for (const auto &entry : result.uniqueReceivingAccountCounts)
            {
                aggregatedResult.uniqueReceivingAccountCounts[entry.first] += entry.second;
            }
        }

        return aggregatedResult;
    }

private:
    std::vector<DataStream<T>> dataBlocks_;
};

#endif // DISTRIBUTED_ANALYSIS_H

#ifndef DATA_STREAM_H
#define DATA_STREAM_H

#include <vector>
#include <algorithm>
#include <unordered_map>

#include "TransactionStatis.hpp"

// 定义DataStream类模板
template <typename T>
class DataStream
{
public:
    // 默认构造函数
    DataStream() = default;

    DataStream(std::vector<T> data) : transactions_(std::move(data)) {}

    // 筛选函数
    DataStream<T> filter() const
    {
        // 使用 Lambda 表达式定义判断条件
        auto isNotSameAccount = [](const T &transaction)
        {
            return transaction.fromAccountID != transaction.toAccountID;
        };
        // 筛选数据
        std::vector<T> filtered;
        std::copy_if(transactions_.begin(), transactions_.end(), std::back_inserter(filtered), isNotSameAccount);
        return DataStream<T>(filtered);
    }

    DataStream<T> map() const
    {
        // 定义汇率映射表
        std::unordered_map<int, double> exchangeRates;
        exchangeRates[0] = 1.0; // 人民币对人民币的汇率
        exchangeRates[1] = 7.8; // 欧元对人民币的汇率
        exchangeRates[2] = 7.1; // 美元对人民币的汇率
        auto mapper = [&exchangeRates](const T &transaction)
        {
            T transactionMap = transaction;
            transactionMap.amount *= exchangeRates[transaction.monetaryType];
            transactionMap.monetaryType = 0;
            return transactionMap;
        };
        std::vector<T> mapped;
        std::transform(transactions_.begin(), transactions_.end(), std::back_inserter(mapped), mapper);
        return DataStream<T>(mapped);
    }

    // 归约函数
    TransactionStats reduce() const
    {
        TransactionStats stats;
        stats.transactionCounts = std::accumulate(transactions_.begin(), transactions_.end(), std::unordered_map<int, int>{}, [](auto &result, const T &transaction)
                                                  {
                                                   result[transaction.fromAccountID]++;
                                                   result[transaction.toAccountID]++;
                                                   return result; });

        stats.totalReceivedAmounts = std::accumulate(transactions_.begin(), transactions_.end(), std::unordered_map<int, double>{}, [](auto &result, const T &transaction)
                                                     {
                                                   result[transaction.toAccountID] += transaction.amount;
                                                   return result; });
        stats.uniqueReceivingAccountCounts = std::accumulate(transactions_.begin(), transactions_.end(), std::unordered_map<int, int>{}, [](auto &result, const T &transaction)
                                                             {
                                                   result[transaction.fromAccountID]++;
                                                   return result; });
        return stats;
    }

    // 按键分组函数
    auto keyBy() const
    {
        // 使用货币类型作为键进行分组
        std::unordered_map<int, std::vector<T>> grouped;
        for (const auto &item : transactions_)
        {
            grouped[item.monetaryType].push_back(item);
        }
        return grouped;
    }

    // 分割数据流并创建块
    std::vector<DataStream<T>> splitIntoBlocks(std::size_t numBlocks) const
    {
        std::vector<DataStream<T>> dataBlocks(numBlocks);

        // 计算块的大小
        std::size_t blockSize = transactions_.size() / numBlocks;
        std::size_t remainder = transactions_.size() % numBlocks;
        auto begin = transactions_.begin();

        for (std::size_t i = 0; i < numBlocks; ++i)
        {
            auto end = begin + blockSize + (i < remainder ? 1 : 0);
            dataBlocks[i] = DataStream<T>(std::vector<T>(begin, end));
            begin = end;
        }

        return dataBlocks;
    }

private:
    std::vector<T> transactions_; // 数据集合
};

#endif // DATA_STREAM_H

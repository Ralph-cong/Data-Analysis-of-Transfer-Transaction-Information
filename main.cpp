#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <random>
#include <unordered_map>
#include <chrono>

#include "TransactionStatis.hpp"
#include "DataStream.hpp"
#include "DistributedAnalysis.hpp"

// 解析字符串函数
Transaction parseTransaction(const std::string &line)
{
    Transaction transaction;
    std::istringstream iss(line);
    iss >> transaction.fromAccountID >> transaction.toAccountID >> transaction.amount >> transaction.monetaryType;
    return transaction;
}

// 解析并创建数据流函数
template <typename T>
DataStream<T> parseAndCreateDataStream(std::ifstream &inputFile)
{
    std::vector<T> transactions;
    std::string line;

    // 从文件中解析交易记录并创建数据流
    while (std::getline(inputFile, line))
    {
        transactions.push_back(parseTransaction(line));
    }

    return DataStream<T>(transactions);
}

// 生成随机交易函数
Transaction generateRandomTransaction(std::mt19937 &gen)
{
    std::uniform_int_distribution<> accountDistribution(1, 30);       // 随机生成的账户ID
    std::uniform_real_distribution<> amountDistribution(1.0, 1500.0); // 随机生成的交易金额
    std::uniform_int_distribution<> monetaryTypeDistribution(0, 2);   // 随机生成的货币类型

    Transaction transaction;
    transaction.fromAccountID = accountDistribution(gen);
    transaction.toAccountID = accountDistribution(gen);

    transaction.monetaryType = monetaryTypeDistribution(gen);
    switch (transaction.monetaryType)
    {
    case 0:
        transaction.amount = amountDistribution(gen);
        break;
    case 1:
        transaction.amount = amountDistribution(gen) / 7.8;
        break;
    case 2:
        transaction.amount = amountDistribution(gen) / 7.1;
        break;
    }

    return transaction;
}

int main()
{

    // 使用当前时间的计数作为种子
    unsigned seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());

    // 创建一个 Mersenne Twister 引擎
    std::mt19937 gen(seed);
    std::uniform_int_distribution<> totalTransactionAmount(80, 120); // 随机生成的交易数据条数
    int transactionAmount = totalTransactionAmount(gen);

    std::ofstream outputFile("transaction_data.txt");
    if (!outputFile.is_open())
    {
        std::cerr << "无法打开文件!\n";
        return 1;
    }

    // 生成随机交易并写入文件
    for (int i = 0; i < transactionAmount; ++i)
    {
        Transaction transaction = generateRandomTransaction(gen);
        outputFile << transaction.fromAccountID << " " << transaction.toAccountID << " " << transaction.amount << " " << transaction.monetaryType << "\n";
    }

    outputFile.close();

    std::ifstream inputFile("transaction_data.txt");
    if (!inputFile.is_open())
    {
        std::cerr << "无法打开文件!\n";
        return 1;
    }

    // 解析文件并创建数据流
    DataStream<Transaction> transactionStream = parseAndCreateDataStream<Transaction>(inputFile);

    // 使用 keyBy 函数按货币类型进行分组
    auto groupedByMonetaryType = transactionStream.keyBy();

    // 筛选数据流, 过滤掉同一账户间的交易的异常数据
    transactionStream = transactionStream.filter();

    // 映射数据流, 进行汇率转换
    transactionStream = transactionStream.map();

    // 将数据流分成多个块
    auto dataBlocks = transactionStream.splitIntoBlocks(4);

    // 创建分布式计算对象
    DistributedAnalysis<Transaction> distributedAnalysis(dataBlocks);

    // 在每个节点上执行数据分析操作
    auto results = distributedAnalysis.analyzeOnNode();

    // 聚合结果
    TransactionStats aggregatedResult = distributedAnalysis.aggregateResults(results);

    auto transactionCounts = aggregatedResult.transactionCounts;
    auto receivedAmounts = aggregatedResult.totalReceivedAmounts;
    auto uniqueReceivingAccounts = aggregatedResult.uniqueReceivingAccountCounts;

    // 分别显示交易次数超过5次、收款总金额超过5000、接收账户数超过两个的交易账户。
    std::cout << "交易次数超过5次的:\n";
    for (const auto &entry : transactionCounts)
    {
        if (entry.second > 5)
            std::cout << "账户 " << entry.first << ": " << entry.second << " 次交易\n";
    }

    std::cout << "\n收款总金额超过5000元的:\n";
    for (const auto &entry : receivedAmounts)
    {
        if (entry.second > 5000)
            std::cout << "账户 " << entry.first << ": ￥" << entry.second << "\n";
    }

    std::cout << "\n接收账户数超过两个的:\n";
    for (const auto &entry : uniqueReceivingAccounts)
    {
        if (entry.second > 2)
            std::cout << "账户 " << entry.first << ": " << entry.second << " 个接收账户\n";
    }

    std::cout << "\n";
    for (const auto &entry : groupedByMonetaryType)
    {
        if (entry.first == 0) // 0 表示人民币
        {
            std::cout << "人民币交易数：" << entry.second.size() << "\n";
        }
        else if (entry.first == 1)
        {
            std::cout << "欧元交易数：" << entry.second.size() << "\n";
        }
        else if (entry.first == 2)
        {
            std::cout << "美元交易数：" << entry.second.size() << "\n";
        }
    }
    return 0;
}

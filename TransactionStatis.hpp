#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <unordered_map>

// 定义交易结构体
struct Transaction
{
    int fromAccountID; // 起始账户ID
    int toAccountID;   // 目标账户ID
    double amount;     // 交易金额
    int monetaryType;  // 货币类型,0代表人民币，1代表欧元，2代表美元
};

// 定义统计分析结果结构体，包含交易次数、收款总金额和接收账户数
struct TransactionStats
{
    std::unordered_map<int, int> transactionCounts;
    std::unordered_map<int, double> totalReceivedAmounts;
    std::unordered_map<int, int> uniqueReceivingAccountCounts;
};

#endif

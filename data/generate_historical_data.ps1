#Requires -Version 5.1

<#
.SYNOPSIS
Generate historical accounting transaction data (Jan 1 - May 26, 2026).
Output: CSV file with ~146,000 synthetic transactions (~1000 per day).
Ensures at least one day has negative profit (expenses > revenue).
#>

param(
    [string]$OutputPath = "historical_transactions.csv"
)

$ErrorActionPreference = "Stop"

# Configuration
$START_DATE = [datetime]::new(2026, 1, 1, 0, 0, 0, [System.DateTimeKind]::Utc)
$END_DATE = [datetime]::new(2026, 5, 26, 0, 0, 0, [System.DateTimeKind]::Utc)
$MIN_TX_PER_DAY = 800
$MAX_TX_PER_DAY = 1201  # exclusive upper bound → 800..1200, avg ~1000

$TRANSACTION_TYPES = @("SALE", "REFUND", "PAYMENT", "FEE", "TRANSFER")
$ENTRY_TYPES = @("DEBIT", "CREDIT")
$STATUSES_WEIGHTED = @(
    "POSTED","POSTED","POSTED","POSTED","POSTED","POSTED","POSTED","POSTED",
    "PENDING","PENDING",
    "REVERSED"
)   # ~73% POSTED, ~18% PENDING, ~9% REVERSED
$ACCOUNT_NAMES = @(
    "Acme Corp", "TechStart Inc", "Global Solutions", "Blue Mountain Ltd",
    "Phoenix Enterprises", "Silver Wave Co", "Golden Gate Corp", "Horizon Tech",
    "Nexus Innovations", "Quantum Systems", "Stellar Industries", "Apex Group",
    "Summit Partners", "Cascade Holdings", "Northwind Traders", "Contoso Ltd",
    "Redwood Analytics", "Palisade Ventures", "Ironclad Security", "Zenith Labs"
)

function New-TransactionObject {
    param(
        [datetime]$Date,
        [bool]$IsRevenue
    )

    $hour = Get-Random -Minimum 8 -Maximum 19       # business hours 08:00-18:59
    $minute = Get-Random -Minimum 0 -Maximum 60
    $second = Get-Random -Minimum 0 -Maximum 60
    $txTimestamp = $Date.AddHours($hour).AddMinutes($minute).AddSeconds($second)

    if ($IsRevenue) {
        $amount = [math]::Round((Get-Random -Minimum 200 -Maximum 8000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 2)
        $category = "REVENUE"
    }
    else {
        $amount = [math]::Round((Get-Random -Minimum 50 -Maximum 6000) + (Get-Random -Minimum 0 -Maximum 100) / 100, 2)
        $category = "EXPENSE"
    }

    [PSCustomObject]@{
        transaction_id        = [guid]::NewGuid().ToString()
        amount                = $amount
        account_name          = $ACCOUNT_NAMES | Get-Random
        transaction_type      = $TRANSACTION_TYPES | Get-Random
        transaction_timestamp = $txTimestamp.ToString("yyyy-MM-ddTHH:mm:ssZ")
        account_category      = $category
        entry_type            = $ENTRY_TYPES | Get-Random
        status                = $STATUSES_WEIGHTED | Get-Random
        ingestion_time        = $txTimestamp.ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
}

Write-Host "Generating historical transactions from $($START_DATE.ToString('yyyy-MM-dd')) to $($END_DATE.ToString('yyyy-MM-dd'))..." -ForegroundColor Cyan
Write-Host "Rate: $MIN_TX_PER_DAY - $($MAX_TX_PER_DAY - 1) transactions per day"

# Use a list for performance (appending to array is slow for thousands of items)
$allTransactions = [System.Collections.Generic.List[PSCustomObject]]::new()
$dailySummaries  = [System.Collections.Generic.List[PSCustomObject]]::new()

# Seasonality rules for negative-profit days:
#   1. First week of every month (days 1-7): negative profit → negative weeks
#   2. 10th of every month: high expense spike (seasonality)
#   3. 20th of every month: high expense spike (seasonality)
# This gives ~9 negative days per month, ~45 total out of 146 days

$currentDate = $START_DATE
$dayCount = 0
while ($currentDate -le $END_DATE) {
    $numTransactions = Get-Random -Minimum $MIN_TX_PER_DAY -Maximum $MAX_TX_PER_DAY

    $dayOfMonth = $currentDate.Day

    # Determine seasonality pattern
    $isFirstWeek   = ($dayOfMonth -ge 1 -and $dayOfMonth -le 7)
    $isExpenseSpike = ($dayOfMonth -eq 10 -or $dayOfMonth -eq 20)
    $isForceNegative = $isFirstWeek -or $isExpenseSpike

    $dayTxs = [System.Collections.Generic.List[PSCustomObject]]::new()

    for ($i = 0; $i -lt $numTransactions; $i++) {
        if ($isFirstWeek) {
            # First week: heavy expense bias (75% expense, 25% revenue)
            # Simulates rent, payroll, subscriptions hitting at month start
            $isRevenue = ((Get-Random -Minimum 0 -Maximum 100) -lt 25)
        }
        elseif ($isExpenseSpike) {
            # 10th & 20th: sharp expense spike (85% expense, 15% revenue)
            # Simulates vendor payments, tax installments
            $isRevenue = ((Get-Random -Minimum 0 -Maximum 100) -lt 15)
        }
        else {
            # Normal business days: revenue-heavy (60% revenue, 40% expense)
            $isRevenue = ((Get-Random -Minimum 0 -Maximum 100) -lt 60)
        }

        $tx = New-TransactionObject -Date $currentDate -IsRevenue $isRevenue
        $allTransactions.Add($tx)
        $dayTxs.Add($tx)
    }

    # Calculate daily summary
    $revenueSum = 0.0
    $expenseSum = 0.0
    foreach ($tx in $dayTxs) {
        if ($tx.status -eq "POSTED") {
            if ($tx.account_category -eq "REVENUE") { $revenueSum += $tx.amount }
            elseif ($tx.account_category -eq "EXPENSE") { $expenseSum += $tx.amount }
        }
    }
    $dailyProfit = [math]::Round($revenueSum - $expenseSum, 2)

    $reason = if ($isFirstWeek) { "1st-week" } elseif ($isExpenseSpike) { "spike-$dayOfMonth" } else { "" }
    $dailySummaries.Add([PSCustomObject]@{
        Date    = $currentDate.ToString("yyyy-MM-dd")
        Revenue = [math]::Round($revenueSum, 2)
        Expense = [math]::Round($expenseSum, 2)
        Profit  = $dailyProfit
        Reason  = $reason
    })

    $dayCount++
    if ($dayCount % 30 -eq 0) {
        Write-Host "  ... processed $dayCount days ($($allTransactions.Count) transactions so far)" -ForegroundColor DarkGray
    }

    $currentDate = $currentDate.AddDays(1)
}

# Print summary statistics
Write-Host ""
Write-Host ("=" * 80)
Write-Host "SUMMARY"
Write-Host ("=" * 80)

$negativeDays = @($dailySummaries | Where-Object { $_.Profit -lt 0 })
$positiveDays = @($dailySummaries | Where-Object { $_.Profit -ge 0 })

Write-Host "  Total days:          $dayCount"
Write-Host "  Total transactions:  $($allTransactions.Count)"
Write-Host "  Avg tx/day:          $([math]::Round($allTransactions.Count / $dayCount, 1))"
Write-Host "  Positive profit days: $($positiveDays.Count)" -ForegroundColor Green
Write-Host "  Negative profit days: $($negativeDays.Count)" -ForegroundColor Red
Write-Host ""

# Print only negative-profit days
Write-Host "Negative profit days:" -ForegroundColor Yellow
Write-Host ("{0,-12} {1,14} {2,14} {3,14} {4,10}" -f "Date", "Revenue", "Expense", "Profit", "Reason")
Write-Host ("-" * 68)
foreach ($summary in $negativeDays) {
    $reason = if ($summary.Reason) { $summary.Reason } else { "natural" }
    Write-Host ("{0,-12} `${1,13:N2} `${2,13:N2} `${3,13:N2} {4,10}" -f $summary.Date, $summary.Revenue, $summary.Expense, $summary.Profit, $reason) -ForegroundColor Red
}

Write-Host ("=" * 80)

# Write to CSV
Write-Host ""
Write-Host "Writing to $OutputPath..."

$sb = [System.Text.StringBuilder]::new(1MB)
[void]$sb.AppendLine("transaction_id,amount,account_name,transaction_type,transaction_timestamp,account_category,entry_type,status,ingestion_time")

foreach ($tx in $allTransactions) {
    # Quote account_name in case it contains commas
    $accountNameQuoted = "`"$($tx.account_name)`""
    [void]$sb.AppendLine("$($tx.transaction_id),$($tx.amount),$accountNameQuoted,$($tx.transaction_type),$($tx.transaction_timestamp),$($tx.account_category),$($tx.entry_type),$($tx.status),$($tx.ingestion_time)")
}

[System.IO.File]::WriteAllText(
    (Join-Path (Get-Location) $OutputPath),
    $sb.ToString(),
    [System.Text.UTF8Encoding]::new($false)  # UTF-8 without BOM
)

$fileSize = [math]::Round((Get-Item $OutputPath).Length / 1KB, 1)
Write-Host "Successfully generated $OutputPath" -ForegroundColor Green
Write-Host "  Rows:      $($allTransactions.Count)"
Write-Host "  File size: ${fileSize} KB"
Write-Host "  Date range: $($START_DATE.ToString('yyyy-MM-dd')) to $($END_DATE.ToString('yyyy-MM-dd'))"

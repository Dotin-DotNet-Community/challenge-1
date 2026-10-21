namespace HighScalePaymentEngine;

public sealed class TransactionEngine : ITransactionEngine
{
    private const string HealthMessage = "Payment engine is healthy.";

    private readonly object _watchdogLock = new();
    private CancellationTokenSource? _watchdogCancellation;
    private Task? _watchdogTask;
    private bool _disposed;

    public void StartHealthWatchdog(
        Action<string> alertLogger,
        TimeSpan checkInterval)
    {
        ArgumentNullException.ThrowIfNull(alertLogger);

        if (checkInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(checkInterval),
                "The check interval must be greater than zero.");
        }

        lock (_watchdogLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_watchdogTask is not null)
            {
                throw new InvalidOperationException("The health watchdog has already been started.");
            }

            _watchdogCancellation = new CancellationTokenSource();
            _watchdogTask = RunWatchdogAsync(
                alertLogger,
                checkInterval,
                _watchdogCancellation.Token);
        }
    }

    public async Task<IReadOnlyList<BatchResult>> BatchAndDispatchWithThrottleAsync(
        IEnumerable<TransactionRequest> requests,
        decimal maxBatchAmount,
        Func<IReadOnlyList<TransactionRequest>, CancellationToken, Task<BatchResult>> batchGatewayCaller,
        int maxConcurrentGatewayCalls,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(requests);
        ArgumentNullException.ThrowIfNull(batchGatewayCaller);

        if (maxBatchAmount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxBatchAmount),
                "The maximum batch amount must be greater than zero.");
        }

        if (maxConcurrentGatewayCalls <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentGatewayCalls),
                "The maximum number of concurrent gateway calls must be greater than zero.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var requestList = requests.ToList();

        if (requestList.Any(request => request is null))
        {
            throw new ArgumentException("The request collection cannot contain null items.", nameof(requests));
        }

        if (requestList.Any(request => request.Amount < 0))
        {
            throw new ArgumentException("Transaction amounts cannot be negative.", nameof(requests));
        }

        if (requestList.Any(request => request.Amount > maxBatchAmount))
        {
            throw new ArgumentException(
                "A transaction amount cannot exceed the maximum batch amount.",
                nameof(requests));
        }

        if (requestList.Count == 0)
        {
            return Array.Empty<BatchResult>();
        }

        var batches = CreateMinimumBatches(requestList, maxBatchAmount, cancellationToken);
        var results = new BatchResult[batches.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, batches.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = maxConcurrentGatewayCalls,
                CancellationToken = cancellationToken
            },
            async (batchIndex, token) =>
            {
                results[batchIndex] = await batchGatewayCaller(batches[batchIndex], token)
                    .ConfigureAwait(false);
            }).ConfigureAwait(false);

        return results;
    }

    public async Task<RiskAssessment[]> EvaluateRiskRulesAsync(
        IEnumerable<Func<CancellationToken, Task<RiskAssessment>>> riskRules,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(riskRules);

        cancellationToken.ThrowIfCancellationRequested();

        var rules = riskRules.ToArray();
        if (rules.Any(rule => rule is null))
        {
            throw new ArgumentException("The rule collection cannot contain null items.", nameof(riskRules));
        }

        var evaluations = rules.Select(rule => InvokeRiskRuleAsync(rule, cancellationToken));
        return await Task.WhenAll(evaluations).ConfigureAwait(false);
    }

    public async Task<TransactionStatus> GetFastestConfirmationAsync(
        IEnumerable<Func<CancellationToken, Task<TransactionStatus>>> nodes,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(nodes);

        cancellationToken.ThrowIfCancellationRequested();

        var nodeList = nodes.ToArray();
        if (nodeList.Any(node => node is null))
        {
            throw new ArgumentException("The node collection cannot contain null items.", nameof(nodes));
        }

        if (nodeList.Length == 0)
        {
            throw new InvalidOperationException("No confirmation nodes were supplied.");
        }

        var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var allTasks = nodeList
            .Select(node => InvokeNodeAsync(node, linkedCancellation.Token))
            .ToArray();
        var pendingTasks = allTasks.ToList();
        var failures = new List<Exception>();

        try
        {
            while (pendingTasks.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var completedTask = await Task.WhenAny(pendingTasks)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                pendingTasks.Remove(completedTask);

                try
                {
                    var result = await completedTask.ConfigureAwait(false);
                    linkedCancellation.Cancel();
                    _ = ObserveTasksAndDisposeAsync(allTasks, linkedCancellation);
                    return result;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    failures.Add(new TaskCanceledException(completedTask));
                }
                catch (Exception exception)
                {
                    failures.Add(exception);
                }
            }
        }
        catch
        {
            linkedCancellation.Cancel();
            _ = ObserveTasksAndDisposeAsync(allTasks, linkedCancellation);
            throw;
        }

        linkedCancellation.Dispose();
        cancellationToken.ThrowIfCancellationRequested();

        throw new InvalidOperationException(
            "All confirmation nodes failed.",
            new AggregateException(failures));
    }

    public IReadOnlyList<EncryptedRecord> EncryptAuditLogsInParallel(
        IEnumerable<RawAuditLog> logs,
        Func<RawAuditLog, EncryptedRecord> encryptAlgorithm,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(logs);
        ArgumentNullException.ThrowIfNull(encryptAlgorithm);

        if (maxDegreeOfParallelism <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDegreeOfParallelism),
                "The maximum degree of parallelism must be greater than zero.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var logArray = logs.ToArray();
        if (logArray.Any(log => log is null))
        {
            throw new ArgumentException("The log collection cannot contain null items.", nameof(logs));
        }

        var encryptedRecords = new EncryptedRecord[logArray.Length];
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = cancellationToken
        };

        Parallel.For(0, logArray.Length, options, index =>
        {
            encryptedRecords[index] = encryptAlgorithm(logArray[index]);
        });

        return encryptedRecords;
    }

    public void Dispose()
    {
        CancellationTokenSource? cancellation;
        Task? watchdogTask;

        lock (_watchdogLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            cancellation = _watchdogCancellation;
            watchdogTask = _watchdogTask;
            _watchdogCancellation = null;
            _watchdogTask = null;
        }

        if (cancellation is null)
        {
            return;
        }

        cancellation.Cancel();

        try
        {
            watchdogTask?.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            cancellation.Dispose();
        }
    }

    private static async Task RunWatchdogAsync(
        Action<string> alertLogger,
        TimeSpan checkInterval,
        CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(checkInterval);

        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            alertLogger(HealthMessage);
        }
    }

    private static async Task<RiskAssessment> InvokeRiskRuleAsync(
        Func<CancellationToken, Task<RiskAssessment>> riskRule,
        CancellationToken cancellationToken)
    {
        return await riskRule(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<TransactionStatus> InvokeNodeAsync(
        Func<CancellationToken, Task<TransactionStatus>> node,
        CancellationToken cancellationToken)
    {
        return await node(cancellationToken).ConfigureAwait(false);
    }

    private static async Task ObserveTasksAndDisposeAsync(
        IEnumerable<Task<TransactionStatus>> tasks,
        CancellationTokenSource cancellation)
    {
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch
        {
        }
        finally
        {
            cancellation.Dispose();
        }
    }

    private static IReadOnlyList<IReadOnlyList<TransactionRequest>> CreateMinimumBatches(
        IReadOnlyList<TransactionRequest> requests,
        decimal maxBatchAmount,
        CancellationToken cancellationToken)
    {
        var sortedRequests = requests
            .Select((request, index) => new IndexedRequest(index, request))
            .OrderByDescending(item => item.Request.Amount)
            .ThenBy(item => item.Index)
            .ToArray();

        var initialAssignment = CreateFirstFitAssignment(sortedRequests, maxBatchAmount, out var bestBatchCount);
        var bestAssignment = initialAssignment;
        var totalAmount = sortedRequests.Sum(item => item.Request.Amount);
        var lowerBound = (int)decimal.Ceiling(totalAmount / maxBatchAmount);

        if (bestBatchCount > lowerBound)
        {
            var currentAssignment = new int[sortedRequests.Length];
            var batchAmounts = new decimal[bestBatchCount];
            var remainingAmounts = new decimal[sortedRequests.Length + 1];

            for (var index = sortedRequests.Length - 1; index >= 0; index--)
            {
                remainingAmounts[index] = remainingAmounts[index + 1] + sortedRequests[index].Request.Amount;
            }

            Search(0, 0);

            void Search(int itemIndex, int usedBatchCount)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (bestBatchCount == lowerBound || usedBatchCount >= bestBatchCount)
                {
                    return;
                }

                if (itemIndex == sortedRequests.Length)
                {
                    bestBatchCount = usedBatchCount;
                    bestAssignment = (int[])currentAssignment.Clone();
                    return;
                }

                var availableCapacity = 0m;
                for (var batchIndex = 0; batchIndex < usedBatchCount; batchIndex++)
                {
                    availableCapacity += maxBatchAmount - batchAmounts[batchIndex];
                }

                var amountRequiringNewBatches = Math.Max(0m, remainingAmounts[itemIndex] - availableCapacity);
                var additionalBatchLowerBound = (int)decimal.Ceiling(amountRequiringNewBatches / maxBatchAmount);
                if (usedBatchCount + additionalBatchLowerBound >= bestBatchCount)
                {
                    return;
                }

                var amount = sortedRequests[itemIndex].Request.Amount;
                var triedBatchAmounts = new HashSet<decimal>();

                for (var batchIndex = 0; batchIndex < usedBatchCount; batchIndex++)
                {
                    if (batchAmounts[batchIndex] + amount > maxBatchAmount ||
                        !triedBatchAmounts.Add(batchAmounts[batchIndex]))
                    {
                        continue;
                    }

                    currentAssignment[itemIndex] = batchIndex;
                    batchAmounts[batchIndex] += amount;
                    Search(itemIndex + 1, usedBatchCount);
                    batchAmounts[batchIndex] -= amount;
                }

                if (usedBatchCount + 1 < bestBatchCount)
                {
                    currentAssignment[itemIndex] = usedBatchCount;
                    batchAmounts[usedBatchCount] = amount;
                    Search(itemIndex + 1, usedBatchCount + 1);
                    batchAmounts[usedBatchCount] = 0;
                }
            }
        }

        var groupedRequests = Enumerable.Range(0, bestBatchCount)
            .Select(_ => new List<IndexedRequest>())
            .ToArray();

        for (var index = 0; index < sortedRequests.Length; index++)
        {
            groupedRequests[bestAssignment[index]].Add(sortedRequests[index]);
        }

        return groupedRequests
            .Select(batch => batch.OrderBy(item => item.Index).ToList())
            .OrderBy(batch => batch[0].Index)
            .Select(batch => (IReadOnlyList<TransactionRequest>)batch.Select(item => item.Request).ToArray())
            .ToArray();
    }

    private static int[] CreateFirstFitAssignment(
        IReadOnlyList<IndexedRequest> sortedRequests,
        decimal maxBatchAmount,
        out int batchCount)
    {
        var assignment = new int[sortedRequests.Count];
        var batchAmounts = new List<decimal>();

        for (var itemIndex = 0; itemIndex < sortedRequests.Count; itemIndex++)
        {
            var amount = sortedRequests[itemIndex].Request.Amount;
            var selectedBatch = -1;

            for (var batchIndex = 0; batchIndex < batchAmounts.Count; batchIndex++)
            {
                if (batchAmounts[batchIndex] + amount <= maxBatchAmount)
                {
                    selectedBatch = batchIndex;
                    break;
                }
            }

            if (selectedBatch < 0)
            {
                selectedBatch = batchAmounts.Count;
                batchAmounts.Add(0);
            }

            assignment[itemIndex] = selectedBatch;
            batchAmounts[selectedBatch] += amount;
        }

        batchCount = batchAmounts.Count;
        return assignment;
    }

    private void ThrowIfDisposed()
    {
        lock (_watchdogLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }
    }

    private sealed record IndexedRequest(int Index, TransactionRequest Request);
}

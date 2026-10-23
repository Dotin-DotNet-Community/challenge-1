using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HighScalePaymentEngine;

public sealed class TransactionEngine : ITransactionEngine
{
    private readonly object _watchdogLock = new();
    private Timer? _watchdog;
    private bool _disposed;

    public void StartHealthWatchdog(Action<string> alertLogger, TimeSpan checkInterval)
    {
        ArgumentNullException.ThrowIfNull(alertLogger);
        if (checkInterval <= TimeSpan.Zero || checkInterval.TotalMilliseconds > 4294967294d)
            throw new ArgumentOutOfRangeException(nameof(checkInterval));

        lock (_watchdogLock)
        {
            ThrowIfDisposed();
            if (_watchdog is not null)
                throw new InvalidOperationException("The watchdog is already running.");

            _watchdog = new Timer(_ =>
            {
                if (!Monitor.TryEnter(_watchdogLock))
                    return;
                try
                {
                    if (_disposed)
                        return;
                    try
                    {
                        alertLogger($"[{DateTimeOffset.UtcNow:O}] Transaction engine watchdog is running.");
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError($"Watchdog logger failed: {ex}");
                    }
                }
                finally
                {
                    Monitor.Exit(_watchdogLock);
                }
            }, null, checkInterval, checkInterval);
        }
    }

    public async Task<IReadOnlyList<BatchResult>> BatchAndDispatchWithThrottleAsync(
        IEnumerable<TransactionRequest> requests,
        decimal maxBatchAmount,
        Func<IReadOnlyList<TransactionRequest>, CancellationToken, Task<BatchResult>> batchGatewayCaller,
        int maxConcurrentGatewayCalls,
        CancellationToken cancellationToken = default)
    {
        EnsureActive();
        ArgumentNullException.ThrowIfNull(requests);
        ArgumentNullException.ThrowIfNull(batchGatewayCaller);
        if (maxBatchAmount <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxBatchAmount));
        if (maxConcurrentGatewayCalls <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentGatewayCalls));

        var items = new List<TransactionRequest>();
        foreach (var request in requests)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (request is null || request.Amount < 0 || request.Amount > maxBatchAmount)
                throw new ArgumentException("Requests must be non-null and amounts must be between zero and the batch limit.", nameof(requests));
            items.Add(request);
        }

        cancellationToken.ThrowIfCancellationRequested();
        var batches = await Task.Run(
            () => PackExactly(items, maxBatchAmount, cancellationToken),
            cancellationToken).ConfigureAwait(false);
        var results = new BatchResult[batches.Count];
        var next = -1;

        async Task WorkerAsync()
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var index = Interlocked.Increment(ref next);
                if (index >= batches.Count)
                    return;
                results[index] = await batchGatewayCaller(batches[index], cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        var workers = Enumerable.Range(0, Math.Min(maxConcurrentGatewayCalls, batches.Count))
            .Select(_ => Task.Run(WorkerAsync, cancellationToken)).ToArray();
        await Task.WhenAll(workers).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return results;
    }

    private static List<IReadOnlyList<TransactionRequest>> PackExactly(
        List<TransactionRequest> requests, decimal capacity, CancellationToken token)
    {
        var items = requests.OrderByDescending(x => x.Amount).ToArray();
        if (items.Length == 0)
            return new List<IReadOnlyList<TransactionRequest>>();

        var best = new List<List<TransactionRequest>>();
        var remaining = new List<decimal>();
        foreach (var item in items)
        {
            token.ThrowIfCancellationRequested();
            var index = remaining.FindIndex(x => x >= item.Amount);
            if (index < 0)
            {
                index = best.Count;
                best.Add(new List<TransactionRequest>());
                remaining.Add(capacity);
            }
            best[index].Add(item);
            remaining[index] -= item.Amount;
        }

        var wholeBins = 0;
        var remainder = 0m;
        foreach (var item in items)
        {
            token.ThrowIfCancellationRequested();
            if (item.Amount >= capacity - remainder)
            {
                wholeBins++;
                remainder = item.Amount - (capacity - remainder);
            }
            else
                remainder += item.Amount;
        }
        var lowerBound = Math.Max(1, wholeBins + (remainder > 0 ? 1 : 0));
        if (best.Count == lowerBound)
            return best.Select(x => (IReadOnlyList<TransactionRequest>)x.AsReadOnly()).ToList();

        var current = new List<List<TransactionRequest>>();
        remaining.Clear();

        void Search(int position)
        {
            token.ThrowIfCancellationRequested();
            if (best.Count == lowerBound)
                return;
            if (position == items.Length)
            {
                if (current.Count < best.Count)
                    best = current.Select(x => new List<TransactionRequest>(x)).ToList();
                return;
            }
            if (current.Count >= best.Count)
                return;

            var item = items[position];
            var tried = new HashSet<decimal>();
            for (var i = 0; i < current.Count; i++)
            {
                var space = remaining[i];
                if (space < item.Amount || !tried.Add(space))
                    continue;
                current[i].Add(item);
                remaining[i] -= item.Amount;
                Search(position + 1);
                remaining[i] = space;
                current[i].RemoveAt(current[i].Count - 1);
            }

            if (current.Count + 1 < best.Count)
            {
                current.Add(new List<TransactionRequest> { item });
                remaining.Add(capacity - item.Amount);
                Search(position + 1);
                remaining.RemoveAt(remaining.Count - 1);
                current.RemoveAt(current.Count - 1);
            }
        }

        Search(0);
        return best.Select(x => (IReadOnlyList<TransactionRequest>)x.AsReadOnly()).ToList();
    }

    public async Task<RiskAssessment[]> EvaluateRiskRulesAsync(
        IEnumerable<Func<CancellationToken, Task<RiskAssessment>>> riskRules,
        CancellationToken cancellationToken = default)
    {
        EnsureActive();
        ArgumentNullException.ThrowIfNull(riskRules);
        cancellationToken.ThrowIfCancellationRequested();
        var rules = riskRules.ToArray();
        if (rules.Any(x => x is null))
            throw new ArgumentException("Rules cannot contain null.", nameof(riskRules));

        var tasks = rules.Select(rule => Task.Run(async () =>
            await rule(cancellationToken).ConfigureAwait(false), cancellationToken)).ToArray();
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return results;
    }

    public async Task<TransactionStatus> GetFastestConfirmationAsync(
        IEnumerable<Func<CancellationToken, Task<TransactionStatus>>> nodes,
        CancellationToken cancellationToken = default)
    {
        EnsureActive();
        ArgumentNullException.ThrowIfNull(nodes);
        cancellationToken.ThrowIfCancellationRequested();
        var callers = nodes.ToArray();
        if (callers.Any(x => x is null))
            throw new ArgumentException("Nodes cannot contain null.", nameof(nodes));
        if (callers.Length == 0)
            throw new InvalidOperationException("No confirmation nodes were provided.");

        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var firstSuccess = new TaskCompletionSource<TransactionStatus>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = callers.Select(node => Task.Run(async () =>
        {
            var result = await node(linked.Token).ConfigureAwait(false);
            firstSuccess.TrySetResult(result);
            return result;
        }, linked.Token)).ToArray();
        var all = Task.WhenAll(tasks);
        try
        {
            await Task.WhenAny(firstSuccess.Task, all).WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (firstSuccess.Task.IsCompletedSuccessfully)
                return await firstSuccess.Task.ConfigureAwait(false);

            var errors = tasks.Where(t => t.IsFaulted)
                .SelectMany(t => t.Exception!.InnerExceptions).ToList();
            errors.AddRange(tasks.Where(t => t.IsCanceled)
                .Select(t => (Exception)new TaskCanceledException(t)));
            throw new InvalidOperationException("All confirmation nodes failed.", new AggregateException(errors));
        }
        finally
        {
            try { linked.Cancel(); }
            catch (AggregateException ex) { Trace.TraceError($"Node cancellation callback failed: {ex}"); }
            _ = ObserveAndDisposeAsync(all, linked);
        }
    }

    private static async Task ObserveAndDisposeAsync(Task<TransactionStatus[]> all, CancellationTokenSource source)
    {
        try { await all.ConfigureAwait(false); }
        catch (Exception) { /* Observe failures from losing nodes. */ }
        finally { source.Dispose(); }
    }

    public IReadOnlyList<EncryptedRecord> EncryptAuditLogsInParallel(
        IEnumerable<RawAuditLog> logs,
        Func<RawAuditLog, EncryptedRecord> encryptAlgorithm,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken = default)
    {
        EnsureActive();
        ArgumentNullException.ThrowIfNull(logs);
        ArgumentNullException.ThrowIfNull(encryptAlgorithm);
        if (maxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
        cancellationToken.ThrowIfCancellationRequested();
        var input = logs.ToArray();
        if (input.Any(x => x is null))
            throw new ArgumentException("Logs cannot contain null.", nameof(logs));
        var output = new EncryptedRecord[input.Length];
        Parallel.For(0, input.Length, new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = maxDegreeOfParallelism
        }, i => output[i] = encryptAlgorithm(input[i]));
        cancellationToken.ThrowIfCancellationRequested();
        return output;
    }

    private void EnsureActive()
    {
        lock (_watchdogLock)
            ThrowIfDisposed();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(TransactionEngine));
    }

    public void Dispose()
    {
        lock (_watchdogLock)
        {
            if (_disposed)
                return;
            _disposed = true;
            _watchdog?.Dispose();
            _watchdog = null;
        }
    }
}

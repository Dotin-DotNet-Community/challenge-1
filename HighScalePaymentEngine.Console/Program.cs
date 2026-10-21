using HighScalePaymentEngine;

namespace HighScalePaymentEngine.ConsoleApp;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        using var applicationCancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            applicationCancellation.Cancel();
        };

        if (args.Length > 0)
        {
            await RunFromArgumentAsync(args[0], applicationCancellation.Token);
            return;
        }

        await ShowMenuAsync(applicationCancellation.Token);
    }

    private static async Task ShowMenuAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine();
            Console.WriteLine("High-Scale Payment Engine");
            Console.WriteLine("-------------------------");
            Console.WriteLine("1. Health watchdog");
            Console.WriteLine("2. Batch and dispatch");
            Console.WriteLine("3. Risk rules");
            Console.WriteLine("4. Fastest confirmation");
            Console.WriteLine("5. Parallel audit encryption");
            Console.WriteLine("6. Run all scenarios");
            Console.WriteLine("0. Exit");
            Console.Write("Select an option: ");

            var selection = Console.ReadLine();
            if (selection == "0" || selection is null)
            {
                return;
            }

            try
            {
                await RunFromArgumentAsync(selection, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine("Operation cancelled.");
                return;
            }
            catch (Exception exception)
            {
                WriteError(exception);
            }
        }
    }

    private static async Task RunFromArgumentAsync(string scenario, CancellationToken cancellationToken)
    {
        switch (scenario.Trim().ToLowerInvariant())
        {
            case "1":
            case "watchdog":
                await RunWatchdogDemoAsync(cancellationToken);
                break;
            case "2":
            case "batch":
                await RunBatchDemoAsync(cancellationToken);
                break;
            case "3":
            case "risk":
                await RunRiskDemoAsync(cancellationToken);
                break;
            case "4":
            case "confirmation":
                await RunConfirmationDemoAsync(cancellationToken);
                break;
            case "5":
            case "encryption":
                RunEncryptionDemo(cancellationToken);
                break;
            case "6":
            case "all":
                await RunAllAsync(cancellationToken);
                break;
            default:
                Console.WriteLine($"Unknown scenario: {scenario}");
                Console.WriteLine("Valid arguments: watchdog, batch, risk, confirmation, encryption, all");
                break;
        }
    }

    private static async Task RunAllAsync(CancellationToken cancellationToken)
    {
        await RunWatchdogDemoAsync(cancellationToken);
        await RunBatchDemoAsync(cancellationToken);
        await RunRiskDemoAsync(cancellationToken);
        await RunConfirmationDemoAsync(cancellationToken);
        RunEncryptionDemo(cancellationToken);
    }

    private static async Task RunWatchdogDemoAsync(CancellationToken cancellationToken)
    {
        WriteHeader("Health watchdog");

        using var engine = new TransactionEngine();
        engine.StartHealthWatchdog(
            message => Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}"),
            TimeSpan.FromMilliseconds(400));

        await Task.Delay(TimeSpan.FromSeconds(1.7), cancellationToken);
        Console.WriteLine("Disposing the engine; no more health messages should appear.");
    }

    private static async Task RunBatchDemoAsync(CancellationToken cancellationToken)
    {
        WriteHeader("Optimal batching with throttled dispatch");

        var requests = new[]
        {
            new TransactionRequest("TX-1001", 6m),
            new TransactionRequest("TX-1002", 5m),
            new TransactionRequest("TX-1003", 3m),
            new TransactionRequest("TX-1004", 2m),
            new TransactionRequest("TX-1005", 2m),
            new TransactionRequest("TX-1006", 2m)
        };

        using var engine = new TransactionEngine();
        var batchNumber = 0;
        var results = await engine.BatchAndDispatchWithThrottleAsync(
            requests,
            maxBatchAmount: 10m,
            async (batch, token) =>
            {
                var currentBatch = Interlocked.Increment(ref batchNumber);
                var ids = string.Join(", ", batch.Select(request => request.Id));
                var total = batch.Sum(request => request.Amount);

                Console.WriteLine($"Gateway started batch {currentBatch}: [{ids}], total = {total}");
                await Task.Delay(500, token);
                Console.WriteLine($"Gateway completed batch {currentBatch}");

                return new BatchResult(currentBatch, batch.Count, total, true);
            },
            maxConcurrentGatewayCalls: 2,
            cancellationToken);

        Console.WriteLine($"Created and dispatched {results.Count} optimal batches.");
        foreach (var result in results)
        {
            Console.WriteLine(
                $"Batch {result.BatchIndex}: items={result.TotalItems}, amount={result.TotalAmount}, success={result.IsSuccess}");
        }
    }

    private static async Task RunRiskDemoAsync(CancellationToken cancellationToken)
    {
        WriteHeader("Concurrent risk evaluation");

        using var engine = new TransactionEngine();
        var rules = new Func<CancellationToken, Task<RiskAssessment>>[]
        {
            token => EvaluateRuleAsync("Blacklist", 600, true, 10, token),
            token => EvaluateRuleAsync("Identity", 350, true, 5, token),
            token => EvaluateRuleAsync("Fraud", 800, false, 78, token)
        };

        var startedAt = DateTime.UtcNow;
        var results = await engine.EvaluateRiskRulesAsync(rules, cancellationToken);
        var elapsed = DateTime.UtcNow - startedAt;

        foreach (var result in results)
        {
            Console.WriteLine(
                $"{result.RuleName}: approved={result.IsApproved}, score={result.RiskScore}");
        }

        Console.WriteLine($"All rules finished in {elapsed.TotalMilliseconds:N0} ms.");
    }

    private static async Task RunConfirmationDemoAsync(CancellationToken cancellationToken)
    {
        WriteHeader("First successful confirmation");

        using var engine = new TransactionEngine();
        var nodes = new Func<CancellationToken, Task<TransactionStatus>>[]
        {
            async token =>
            {
                await Task.Delay(250, token);
                Console.WriteLine("Node A failed.");
                throw new InvalidOperationException("Node A is unavailable.");
            },
            async token =>
            {
                await Task.Delay(450, token);
                Console.WriteLine("Node B confirmed the transaction.");
                return new TransactionStatus("TX-1001", "Confirmed");
            },
            async token =>
            {
                try
                {
                    await Task.Delay(2_000, token);
                    return new TransactionStatus("TX-1001", "Confirmed by slow node");
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Node C was cancelled after another node succeeded.");
                    throw;
                }
            }
        };

        var status = await engine.GetFastestConfirmationAsync(nodes, cancellationToken);
        Console.WriteLine($"Selected result: transaction={status.TransactionId}, state={status.State}");

        await Task.Delay(100, cancellationToken);
    }

    private static void RunEncryptionDemo(CancellationToken cancellationToken)
    {
        WriteHeader("Parallel audit-log encryption");

        var logs = Enumerable.Range(1, 12)
            .Select(index => new RawAuditLog(index, $"payment-audit-{index}"))
            .ToArray();

        using var engine = new TransactionEngine();
        var results = engine.EncryptAuditLogsInParallel(
            logs,
            log =>
            {
                Thread.Sleep(100);
                var encryptedPayload = Convert.ToBase64String(
                    System.Text.Encoding.UTF8.GetBytes(log.Payload));

                return new EncryptedRecord(
                    log.Id,
                    encryptedPayload,
                    Environment.CurrentManagedThreadId);
            },
            maxDegreeOfParallelism: 3,
            cancellationToken);

        foreach (var result in results)
        {
            Console.WriteLine(
                $"Log {result.Id}: thread={result.ProcessedByThreadId}, encrypted={result.EncryptedPayload}");
        }
    }

    private static async Task<RiskAssessment> EvaluateRuleAsync(
        string ruleName,
        int delayMilliseconds,
        bool isApproved,
        int riskScore,
        CancellationToken cancellationToken)
    {
        Console.WriteLine($"Rule {ruleName} started.");
        await Task.Delay(delayMilliseconds, cancellationToken);
        Console.WriteLine($"Rule {ruleName} completed.");
        return new RiskAssessment(ruleName, isApproved, riskScore);
    }

    private static void WriteHeader(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {title} ===");
    }

    private static void WriteError(Exception exception)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"{exception.GetType().Name}: {exception.Message}");
        Console.ResetColor();
    }
}

# چالش موتور پردازش تراکنش‌های مالی (High-Scale Payment Engine)

## 📌 سناریوی کسب‌وکار
در سامانه‌های پردازش مالی با بار ترافیکی بالا، مدیریت صحیح پردازش‌های همزمان و بهینه‌سازی مصرف منابع اهمیت ویژه‌ای دارد. هدف این چالش، پیاده‌سازی کلاس `TransactionEngine` بر اساس اینترفیس `ITransactionEngine` است تا نیازمندی‌های زیر با رعایت پایداری و استانداردهای کیفی پوشش داده شوند.

---

## 🎯 شرح متدها و نیازمندی‌ها

### ۱. StartHealthWatchdog
پایشگری را راه‌اندازی می‌کند که در بازه‌های زمانی مشخص (`checkInterval`) وضعیت سلامت سامانه را با `alertLogger` ثبت کند. با فراخوانی متد `Dispose` در کلاس، این پایشگر باید به شکل تمیز خاتمه یابد.

### ۲. BatchAndDispatchWithThrottleAsync
- **تجمیع:** تراکنش‌های ورودی باید به دسته‌هایی تفکیک شوند به‌گونه‌ای که مجموع مبلغ هر دسته حداکثر برابر با `maxBatchAmount` باشد و تعداد دسته‌ها به حداقل برسد. اگر مبلغ هر تراکنش به تنهایی بیشتر از این مقدار باشد، متد باید `ArgumentException` پرتاب کند.
- **ارسال:** دسته‌های ایجادشده توسط متد `batchGatewayCaller` ارسال می‌شوند. در هر لحظه، حداکثر `maxConcurrentGatewayCalls` بسته اجازه پردازش همزمان دارند. خروجی نهایی، لیست نتایج برگشتی از تمام بسته‌ها است.

### ۳. EvaluateRiskRulesAsync
مجموعه‌ای از قوانین اعتبارسنجی مستقل (`riskRules`) را اجرا کرده و آرایه‌ای از تمام ارزیابی‌های انجام‌شده را برمی‌گرداند.

### ۴. GetFastestConfirmationAsync
وضعیت تراکنش را از چندین نود (`nodes`) استعلام کرده و نتیجه نخستین پاسخی را که با موفقیت تکمیل شد، به عنوان خروجی بازمی‌گرداند. در صورتی که همه نودها با خطا مواجه شوند، متد باید `InvalidOperationException` پرتاب کند.

### ۵. EncryptAuditLogsInParallel
لاگ‌های خام ورودی را با استفاده از تابع `encryptAlgorithm` رمزنگاری می‌کند. این پردازش باید با رعایت سقف همزمانی `maxDegreeOfParallelism` انجام شده و کلیه رکوردهای خروجی بازگردانده شوند.

---

## 🏗 تعاریف مدل‌ها و اینترفیس

```csharp
namespace HighScalePaymentEngine;

public interface ITransactionEngine : IDisposable
{
    void StartHealthWatchdog(
        Action<string> alertLogger, 
        TimeSpan checkInterval);

    Task<IReadOnlyList<BatchResult>> BatchAndDispatchWithThrottleAsync(
        IEnumerable<TransactionRequest> requests,
        decimal maxBatchAmount,
        Func<IReadOnlyList<TransactionRequest>, CancellationToken, Task<BatchResult>> batchGatewayCaller,
        int maxConcurrentGatewayCalls,
        CancellationToken cancellationToken = default);

    Task<RiskAssessment[]> EvaluateRiskRulesAsync(
        IEnumerable<Func<CancellationToken, Task<RiskAssessment>>> riskRules,
        CancellationToken cancellationToken = default);

    Task<TransactionStatus> GetFastestConfirmationAsync(
        IEnumerable<Func<CancellationToken, Task<TransactionStatus>>> nodes,
        CancellationToken cancellationToken = default);

    IReadOnlyList<EncryptedRecord> EncryptAuditLogsInParallel(
        IEnumerable<RawAuditLog> logs,
        Func<RawAuditLog, EncryptedRecord> encryptAlgorithm,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken = default);
}

public record TransactionRequest(string Id, decimal Amount);
public record BatchResult(int BatchIndex, int TotalItems, decimal TotalAmount, bool IsSuccess);
public record RiskAssessment(string RuleName, bool IsApproved, int RiskScore);
public record TransactionStatus(string TransactionId, string State);
public record RawAuditLog(long Id, string Payload);
public record EncryptedRecord(long Id, string EncryptedPayload, int ProcessedByThreadId);
```

---

## 🚀 روال اجرا
کلاس `TransactionEngine` را پیاده‌سازی کرده و پس از اطمینان از کامپایل بدون خطای پروژه، تغییرات خود را در قالب یک Pull Request ارسال کنید.

## 🧪 اجرای نمونه کنسولی

برای مشاهده منوی تعاملی برنامه، دستور زیر را در ریشه مخزن اجرا کنید:

```powershell
dotnet run --project HighScalePaymentEngine.Console
```

برای اجرای خودکار تمام سناریوها بدون نمایش منو:

```powershell
dotnet run --project HighScalePaymentEngine.Console -- all
```

هر سناریو را می‌توان به‌صورت مستقل نیز اجرا کرد:

```powershell
dotnet run --project HighScalePaymentEngine.Console -- watchdog
dotnet run --project HighScalePaymentEngine.Console -- batch
dotnet run --project HighScalePaymentEngine.Console -- risk
dotnet run --project HighScalePaymentEngine.Console -- confirmation
dotnet run --project HighScalePaymentEngine.Console -- encryption
```

برای لغو اجرای یک سناریو از `Ctrl+C` استفاده کنید.

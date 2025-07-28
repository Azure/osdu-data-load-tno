using OSDU.DataLoad.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// Retry policy implementation with exponential backoff
/// </summary>
public class ExponentialRetryPolicy : IRetryPolicy
{
    private readonly ILogger<ExponentialRetryPolicy> _logger;
    private readonly OsduConfiguration _configuration;

    public ExponentialRetryPolicy(
        ILogger<ExponentialRetryPolicy> logger,
        IOptions<OsduConfiguration> configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        var maxRetries = _configuration.RetryCount;
        var baseDelay = _configuration.RetryDelay;

        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                if (attempt > 0)
                {
                    _logger.LogInformation("Retry attempt {Attempt} of {MaxRetries}", attempt, maxRetries);
                }

                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries && ShouldRetry(ex, attempt))
            {
                var delay = CalculateDelay(baseDelay, attempt);
                _logger.LogWarning(ex, "Operation failed on attempt {Attempt}, retrying in {Delay}ms", 
                    attempt + 1, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Operation failed after {Attempts} attempts", attempt + 1);
                throw;
            }
        }

        throw new InvalidOperationException("Should not reach here");
    }

    public bool ShouldRetry(Exception exception, int attemptNumber)
    {
        // Don't retry beyond configured limit
        if (attemptNumber >= _configuration.RetryCount)
            return false;

        // Retry on specific exception types
        return exception switch
        {
            HttpRequestException => true,
            TaskCanceledException => false, // Don't retry on cancellation
            ArgumentException => false, // Don't retry on validation errors
            UnauthorizedAccessException => false, // Don't retry on auth errors
            System.Net.WebException webEx => IsRetryableWebException(webEx),
            _ => false
        };
    }

    private bool IsRetryableWebException(System.Net.WebException webException)
    {
        return webException.Status switch
        {
            System.Net.WebExceptionStatus.Timeout => true,
            System.Net.WebExceptionStatus.ConnectFailure => true,
            System.Net.WebExceptionStatus.ReceiveFailure => true,
            System.Net.WebExceptionStatus.SendFailure => true,
            System.Net.WebExceptionStatus.NameResolutionFailure => true,
            System.Net.WebExceptionStatus.ProxyNameResolutionFailure => true,
            _ => false
        };
    }

    private TimeSpan CalculateDelay(TimeSpan baseDelay, int attempt)
    {
        // Exponential backoff with jitter
        var exponentialDelay = TimeSpan.FromMilliseconds(
            baseDelay.TotalMilliseconds * Math.Pow(2, attempt));

        // Add random jitter (±25%)
        var random = new Random();
        var jitter = random.NextDouble() * 0.5 + 0.75; // 0.75 to 1.25
        
        return TimeSpan.FromMilliseconds(exponentialDelay.TotalMilliseconds * jitter);
    }
}

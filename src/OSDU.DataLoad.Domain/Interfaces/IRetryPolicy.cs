namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for retry policy operations
/// </summary>
public interface IRetryPolicy
{
    /// <summary>
    /// Executes an operation with retry logic
    /// </summary>
    Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Determines if an exception should trigger a retry
    /// </summary>
    bool ShouldRetry(Exception exception, int attemptNumber);
}

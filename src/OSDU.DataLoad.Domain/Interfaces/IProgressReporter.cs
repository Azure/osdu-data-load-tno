using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for reporting progress during long-running operations
/// Provides structured progress updates with time estimates and context
/// </summary>
public interface IProgressReporter<T>
{
    /// <summary>
    /// Report current progress
    /// </summary>
    /// <param name="progress">Current progress information</param>
    void Report(T progress);
}

/// <summary>
/// Specific progress reporter for manifest generation operations
/// </summary>
public interface IManifestProgressReporter : IProgressReporter<ManifestProgress>
{
    /// <summary>
    /// Report a phase change (e.g., loading CSV, processing templates, generating files)
    /// </summary>
    /// <param name="phase">The current phase of operation</param>
    /// <param name="phaseDescription">Description of what's happening in this phase</param>
    void ReportPhaseChange(ManifestGenerationPhase phase, string phaseDescription);

    /// <summary>
    /// Report an error during processing
    /// </summary>
    /// <param name="error">Error information</param>
    void ReportError(ManifestProcessingError error);

    /// <summary>
    /// Report completion of the operation
    /// </summary>
    /// <param name="finalResult">Final operation results</param>
    void ReportCompletion(ManifestGenerationResult finalResult);
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;

namespace MercuryCache.ML
{
    /// <summary>
    /// Serves the ML-based admission model.
    ///
    /// Architecture:
    ///  1. Training is done offline in Python (ml/training/train_admission_model.py)
    ///     using XGBoost / LightGBM and exported to ONNX.
    ///  2. This service loads the ONNX model at startup and serves predictions
    ///     for the C# admission gate, or re-exports the model for C++ ONNX Runtime.
    ///
    /// Inspired by: "LeCaR: Driving Cache Replacement with ML" (USENIX 2018)
    /// and the broader learned-index / learned-cache literature.
    /// </summary>
    public class AdmissionModelService
    {
        private InferenceSession?  _session;
        private readonly string    _modelPath;
        private readonly ILogger<AdmissionModelService> _log;
        private readonly float     _admitThreshold;

        private int  _totalPredictions = 0;
        private int  _admitCount       = 0;

        public bool IsLoaded => _session != null;

        public AdmissionModelService(
            string modelPath        = "ml/exported_models/admission_model.onnx",
            float  admitThreshold   = 0.55f,
            ILogger<AdmissionModelService>? log = null)
        {
            _modelPath      = modelPath;
            _admitThreshold = admitThreshold;
            _log            = log ?? Microsoft.Extensions.Logging.Abstractions
                                              .NullLogger<AdmissionModelService>.Instance;
        }

        /// <summary>Load the ONNX model from disk.</summary>
        public void Load()
        {
            if (!File.Exists(_modelPath))
            {
                _log.LogWarning("Admission model not found at {Path}. "
                    + "Falling back to TinyLFU-only admission.", _modelPath);
                return;
            }
            try
            {
                var options = new SessionOptions();
                options.GraphOptimizationLevel = GraphOptimizationLevel.ORT_ENABLE_EXTENDED;
                _session = new InferenceSession(_modelPath, options);
                _log.LogInformation("Admission model loaded from {Path}", _modelPath);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to load admission model from {Path}", _modelPath);
            }
        }

        /// <summary>
        /// Score a single admission decision.
        /// Returns probability that admitting the incoming entry improves future hit rate.
        /// </summary>
        public float Predict(float[] features)
        {
            if (_session == null) return 0.5f; // neutral — defer to TinyLFU

            var tensor = new DenseTensor<float>(features, new[] { 1, FeatureSchema.FeatureCount });
            var inputs = new List<NamedOnnxValue>
            {
                NamedOnnxValue.CreateFromTensor("features", tensor)
            };

            using var results = _session.Run(inputs);
            var probs = results.First(r => r.Name == "probabilities")
                               .AsTensor<float>()
                               .ToArray();

            float score = probs.Length >= 2 ? probs[1] : probs[0];
            Interlocked.Increment(ref _totalPredictions);
            if (score >= _admitThreshold) Interlocked.Increment(ref _admitCount);
            return score;
        }

        public bool ShouldAdmit(float[] features)
            => Predict(features) >= _admitThreshold;

        public (int Total, int Admitted, double AdmitRate) GetStats()
        {
            int t = _totalPredictions, a = _admitCount;
            return (t, a, t > 0 ? (double)a / t : 0.0);
        }

        public void Dispose() => _session?.Dispose();
    }
}

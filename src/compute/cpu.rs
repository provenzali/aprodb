use rayon::prelude::*;

use crate::Metric;

pub(crate) fn score_vectors_cpu(vectors: &[Vec<f32>], query: &[f32], metric: Metric) -> Vec<f32> {
    vectors
        .par_iter()
        .map(|vector| match metric {
            Metric::Dot => vector
                .iter()
                .zip(query)
                .map(|(left, right)| left * right)
                .sum(),
            Metric::Cosine => {
                let (dot, norm_vector, norm_query) = vector.iter().zip(query).fold(
                    (0.0f32, 0.0f32, 0.0f32),
                    |(dot, nv, nq), (left, right)| {
                        (dot + left * right, nv + left * left, nq + right * right)
                    },
                );
                let denominator = (norm_vector * norm_query).sqrt();
                if denominator > 0.0 {
                    dot / denominator
                } else {
                    0.0
                }
            }
        })
        .collect()
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only

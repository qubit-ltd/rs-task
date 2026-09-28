// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::proptest;
use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::engine::TaskExecutionEngine;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::TaskId;

proptest! {
    #[test]
    fn test_unique_gpu_labels_within_limits_are_accepted(count in 0_usize..=32, suffix in 0_u32..100_000) {
        let labels = (0..count)
            .map(|index| format!("gpu-{suffix}-{index}"))
            .collect::<Vec<_>>();
        let request = ResourceRequest {
            gpu_count: u32::from(count > 0),
            gpu_labels: labels,
            ..ResourceRequest::default()
        };
        prop_assert!(request.validate_limits().is_ok());
    }

    #[test]
    fn test_duplicated_gpu_labels_are_rejected(label in "[a-zA-Z][a-zA-Z0-9_-]{0,31}") {
        let request = ResourceRequest {
            gpu_count: 1,
            gpu_labels: vec![label.clone(), label],
            ..ResourceRequest::default()
        };
        prop_assert!(request.validate_limits().is_err());
    }

    #[test]
    fn test_dropped_cpu_reservations_restore_capacity(cpu_slots in 0_u32..=128, attempts in 1_usize..=64) {
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots,
            ..ResourceCapacity::default()
        });
        for _ in 0..attempts {
            let prepared = engine
                .try_prepare(
                    TaskId::generate(),
                    ResourceRequest {
                        cpu_slots,
                        ..ResourceRequest::default()
                    },
                )
                .expect("a request matching total capacity is reservable");
            prop_assert_eq!(engine.capacity().used_cpu_slots, cpu_slots);
            drop(prepared);
            prop_assert_eq!(engine.capacity().used_cpu_slots, 0);
        }
    }
}

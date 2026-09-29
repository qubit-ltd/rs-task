// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Represents gate lifecycle state and the scope-bound active-operation permit.
mod admission_permit;
mod close_failure;
mod gate_state;
mod phase;

pub(in crate::service) use admission_permit::AdmissionPermit;
pub(in crate::service::admission_gate) use close_failure::CloseFailure;
pub(in crate::service::admission_gate) use gate_state::GateState;
pub(in crate::service::admission_gate) use phase::Phase;

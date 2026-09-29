// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Represents budget usage, failed reservations, and scoped reservation cleanup.
mod admission_budget_error;
mod admission_reservation;
mod budget_usage;

pub(in crate::service) use admission_budget_error::AdmissionBudgetError;
pub(in crate::service) use admission_reservation::AdmissionReservation;
pub(in crate::service) use budget_usage::BudgetUsage;

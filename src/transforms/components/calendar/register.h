#pragma once
// Calendar effect transforms registration
// Provides calendar-based trading anomaly detection
//
// Categories:
// 1. Monthly Effects - Patterns around month boundaries
//    - turn_of_month: Last/first N trading days of month
//    - week_of_month: First/last week patterns
// 2. Weekly Effects - Day-of-week patterns
//    - day_of_week: Monday effect, Friday effect, etc.
// 3. Seasonal Effects - Annual calendar patterns
//    - month_of_year: January effect, sell-in-may, etc.
//    - quarter: Quarter-end effects
// 4. Holiday Effects - Trading around holidays
//    - holiday: Days before/after market holidays
// 5. Intraday Effects - Time-of-day patterns
//    - time_of_day: Opening/closing hour effects

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "calendar_effect.h"
#include "time_of_day.h"

namespace epoch_script::transform::calendar {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all calendar effect transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // MONTHLY EFFECTS
    // =========================================================================
    // Trading patterns related to month boundaries

    // turn_of_month: Boolean flag for last N / first N trading days of month
    // Options: days_before (last N days), days_after (first N days)
    // Outputs: result (boolean - true when in turn-of-month window)
    // Use for: Turn-of-month effect (historically positive bias)
    epoch_script::transform::Register<TurnOfMonthEffect>("turn_of_month");

    // week_of_month: Boolean flag for specific week of month
    // Options: target_value (1=first week, -1=last week, etc.)
    // Outputs: result (boolean)
    // Use for: Options expiration week effects (3rd week)
    epoch_script::transform::Register<WeekOfMonthEffect>("week_of_month");

    // =========================================================================
    // WEEKLY EFFECTS
    // =========================================================================
    // Day-of-week trading patterns

    // day_of_week: Boolean flag for specific weekday
    // Options: target_value (0=Monday, 1=Tuesday, ..., 4=Friday)
    // Outputs: result (boolean)
    // Use for: Monday effect (historically negative), Friday effect
    epoch_script::transform::Register<DayOfWeekEffect>("day_of_week");

    // =========================================================================
    // SEASONAL EFFECTS
    // =========================================================================
    // Annual calendar patterns

    // month_of_year: Boolean flag for specific month
    // Options: target_value (1=January, ..., 12=December)
    // Outputs: result (boolean)
    // Use for: January effect, sell-in-May, Santa rally (December)
    epoch_script::transform::Register<MonthOfYearEffect>("month_of_year");

    // quarter: Boolean flag for specific quarter
    // Options: target_value (1=Q1, 2=Q2, 3=Q3, 4=Q4)
    // Outputs: result (boolean)
    // Use for: Quarter-end rebalancing, window dressing effects
    epoch_script::transform::Register<QuarterEffect>("quarter");

    // =========================================================================
    // HOLIDAY EFFECTS
    // =========================================================================
    // Trading patterns around market holidays

    // holiday: Boolean flag for days before/after holidays
    // Options: days_before, days_after, country (US default)
    // Outputs: result (boolean)
    // Use for: Pre-holiday rally, post-holiday effects
    epoch_script::transform::Register<HolidayEffect>("holiday");

    // =========================================================================
    // INTRADAY EFFECTS
    // =========================================================================
    // Time-of-day trading patterns

    // time_of_day: Boolean flag for specific hour ranges
    // Options: start_hour, end_hour
    // Outputs: result (boolean)
    // Use for: Opening hour volatility, lunch lull, closing auction
    epoch_script::transform::Register<TimeOfDay>("time_of_day");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Calendar effects metadata
    for (const auto& metadata : epoch_script::transforms::MakeCalendarEffectMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::calendar

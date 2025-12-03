#pragma once
// Cross-Sectional Transform Metadata
// Provides metadata for cross-sectional transforms that operate across assets at each timestamp

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// CROSS-SECTIONAL MOMENTUM
// =============================================================================

inline TransformsMetaData MakeCSMomentumMetaData() {
    return TransformsMetaData{
        .id = "cs_momentum",
        .category = TransformCategory::Momentum,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Cross-Sectional Momentum",
        .isCrossSectional = true,
        .desc = "Calculates momentum turns across multiple assets in the same time period, "
                "enabling relative performance comparison within a universe of securities.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Returns",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"indicator", "cross-sectional", "momentum", "relative-performance", "multi-asset", "portfolio-rotation"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-rotation", "relative-strength", "long-short-equity", "factor-investing"},
        .relatedTransforms = {"top_k", "top_k_percent", "bottom_k", "bottom_k_percent"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Use for portfolio rotation strategies to identify relative strength/weakness "
                        "across assets. Ranks assets by momentum to select top performers for long "
                        "positions and bottom performers for short. Requires universe of at least 10+ assets.",
        .limitations = "CRITICAL: Requires multiple assets to compare. Will not work on single-asset "
                       "strategies. Performance degrades with universe size below 10 assets."
    };
}

// =============================================================================
// TOP/BOTTOM K SELECTION
// =============================================================================

inline TransformsMetaData MakeTopKMetaData() {
    return TransformsMetaData{
        .id = "top_k",
        .category = TransformCategory::Momentum,
        .name = "Top K Assets",
        .options = {epoch_script::MetaDataOption{
            .id = "k",
            .name = "Count",
            .type = epoch_core::MetaDataOptionType::Integer,
            .defaultValue = epoch_script::MetaDataOptionDefinition(10.0),
            .min = 1,
            .max = 1000,
            .step_size = 1,
            .desc = "Number of top assets to select",
            .tuningGuidance = "Smaller K (3-5) for concentrated portfolios with higher volatility. "
                              "Larger K (10-30) for diversification."}},
        .isCrossSectional = true,
        .desc = "Selects the top K assets based on their values. Useful for identifying the "
                "best performing assets in a universe.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"indicator", "top-k", "assets", "performance", "selection", "best", "portfolio-rotation"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-rotation", "long-only", "top-performers"},
        .relatedTransforms = {"cs_momentum", "top_k_percent", "bottom_k"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Filter assets for long-only portfolio strategies. Combine with cs_momentum "
                        "or other ranking metrics to select top performers.",
        .limitations = "Requires universe larger than K. Fixed count may not adapt well to "
                       "changing market conditions. Consider top_k_percent for dynamic sizing."
    };
}

inline TransformsMetaData MakeBottomKMetaData() {
    return TransformsMetaData{
        .id = "bottom_k",
        .category = TransformCategory::Momentum,
        .name = "Bottom K Assets",
        .options = {epoch_script::MetaDataOption{
            .id = "k",
            .name = "Count",
            .type = epoch_core::MetaDataOptionType::Integer,
            .defaultValue = epoch_script::MetaDataOptionDefinition(10.0),
            .min = 1,
            .max = 1000,
            .step_size = 1,
            .desc = "Number of bottom assets to select",
            .tuningGuidance = "Smaller K (3-5) for concentrated short positions (higher risk). "
                              "Larger K (10-30) for diversification."}},
        .isCrossSectional = true,
        .desc = "Selects the bottom K assets based on their values. Useful for identifying the "
                "worst performing assets in a universe.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"indicator", "bottom-k", "assets", "performance", "selection", "worst", "portfolio-rotation", "short"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-rotation", "long-short-equity", "short-only", "bottom-performers"},
        .relatedTransforms = {"cs_momentum", "bottom_k_percent", "top_k"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Filter assets for short-only or long-short portfolio strategies. Combine with "
                        "cs_momentum to select worst performers for short positions.",
        .limitations = "Requires universe larger than K. Fixed count may not adapt to market conditions. "
                       "Consider bottom_k_percent for dynamic sizing. Shorting requires additional risk controls."
    };
}

inline TransformsMetaData MakeTopKPercentMetaData() {
    return TransformsMetaData{
        .id = "top_k_percent",
        .category = TransformCategory::Momentum,
        .name = "Top %K of Assets",
        .options = {epoch_script::MetaDataOption{
            .id = "k",
            .name = "Percent",
            .type = epoch_core::MetaDataOptionType::Integer,
            .defaultValue = epoch_script::MetaDataOptionDefinition(10.0),
            .min = 1,
            .max = 100,
            .step_size = 1,
            .desc = "Percentage of top assets to select (1-100)",
            .tuningGuidance = "10-20% for concentrated strategies. 30-40% for moderate "
                              "diversification. 50%+ typically too broad for momentum strategies."}},
        .isCrossSectional = true,
        .desc = "Selects the top K percent of assets based on their values. Useful for identifying the "
                "best performing assets in a universe.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"indicator", "top-k", "assets", "performance", "selection", "best", "portfolio-rotation", "adaptive"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-rotation", "long-only", "adaptive-sizing", "top-performers"},
        .relatedTransforms = {"cs_momentum", "top_k", "bottom_k_percent"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Dynamic portfolio sizing that adapts to universe size. Select top percentage "
                        "for long positions. Better than top_k when universe size changes.",
        .limitations = "Requires minimum universe size for meaningful results (recommend 20+ assets for 10%). "
                       "Very small percentages (<5%) may result in too few holdings."
    };
}

inline TransformsMetaData MakeBottomKPercentMetaData() {
    return TransformsMetaData{
        .id = "bottom_k_percent",
        .category = TransformCategory::Momentum,
        .name = "Bottom %K of Assets",
        .options = {epoch_script::MetaDataOption{
            .id = "k",
            .name = "Percent",
            .type = epoch_core::MetaDataOptionType::Integer,
            .defaultValue = epoch_script::MetaDataOptionDefinition(10.0),
            .min = 1,
            .max = 100,
            .step_size = 1,
            .desc = "Percentage of bottom assets to select (1-100)",
            .tuningGuidance = "10-20% for concentrated short strategies. Match with top_k_percent "
                              "for balanced long-short (e.g., both 20%)."}},
        .isCrossSectional = true,
        .desc = "Selects the bottom K percent of assets based on their values. Useful for identifying "
                "the worst performing assets in a universe.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"indicator", "bottom-k", "assets", "performance", "selection", "worst", "portfolio-rotation", "short", "adaptive"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-rotation", "long-short-equity", "short-only", "adaptive-sizing", "bottom-performers"},
        .relatedTransforms = {"cs_momentum", "bottom_k", "top_k_percent"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Dynamic short portfolio sizing. Select bottom percentage for short or avoidance "
                        "strategies. Scales automatically with universe size changes.",
        .limitations = "Requires minimum universe size (recommend 20+ assets for 10%). Shorting worst "
                       "performers can have limited upside and unlimited downside risk."
    };
}

// =============================================================================
// CROSS-SECTIONAL Z-SCORE
// =============================================================================

inline TransformsMetaData MakeCSZscoreMetaData() {
    return TransformsMetaData{
        .id = "cs_zscore",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Cross-Sectional Z-Score",
        .isCrossSectional = true,
        .desc = "Normalizes each asset's value ACROSS assets at each timestamp, not over time. "
                "At each point in time, calculates: z_i = (value_i - mean_across_assets) / std_across_assets.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Asset Values",
                    .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"cross-sectional", "normalization", "zscore", "factor-scoring", "statistical", "multi-asset", "outlier-detection"},
        .requiresTimeFrame = false,
        .strategyTypes = {"factor-investing", "statistical-arbitrage", "mean-reversion", "cross-sectional", "pairs-trading", "relative-value"},
        .relatedTransforms = {"zscore", "cs_momentum", "top_k", "bottom_k"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Fundamental for cross-sectional strategies and factor investing. Normalize metrics "
                        "(returns, momentum, P/E ratios, volatility) across assets for fair comparison. "
                        "Unlike regular zscore which normalizes over TIME, this normalizes across ASSETS.",
        .limitations = "CRITICAL: Requires multiple assets (minimum 3+, recommended 10+). Different from "
                       "time-series zscore - normalizes ACROSS assets not over time."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeCrossSectionalMetaData() {
    return {
        MakeCSMomentumMetaData(),
        MakeTopKMetaData(),
        MakeBottomKMetaData(),
        MakeTopKPercentMetaData(),
        MakeBottomKPercentMetaData(),
        MakeCSZscoreMetaData()
    };
}

}  // namespace epoch_script::transforms

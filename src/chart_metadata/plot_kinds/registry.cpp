#include "registry.h"

// Include all builder headers
#include "builders/macd_builder.h"
#include "builders/bbands_builder.h"
#include "builders/single_value_builder.h"
#include "builders/aroon_builder.h"
#include "builders/stoch_builder.h"
#include "builders/fisher_builder.h"
#include "builders/qqe_builder.h"
#include "builders/elders_builder.h"
#include "builders/fosc_builder.h"
#include "builders/vortex_builder.h"
#include "builders/ichimoku_builder.h"
#include "builders/chande_kroll_stop_builder.h"
#include "builders/pivot_point_sr_builder.h"
#include "builders/previous_high_low_builder.h"
#include "builders/retracements_builder.h"
#include "builders/gap_builder.h"
#include "builders/shl_builder.h"
#include "builders/bos_choch_builder.h"
#include "builders/order_blocks_builder.h"
#include "builders/fvg_builder.h"
#include "builders/liquidity_builder.h"
#include "builders/sessions_builder.h"
#include "builders/trade_signal_builder.h"
#include "builders/flag_builder.h"
#include "builders/zone_builder.h"
#include "builders/close_line_builder.h"
#include "builders/pivot_point_detector_builder.h"
#include "builders/flag_pattern_builder.h"
#include "builders/pennant_pattern_builder.h"
#include "builders/triangle_patterns_builder.h"
#include "builders/consolidation_box_builder.h"
#include "builders/double_top_bottom_builder.h"
#include "builders/head_and_shoulders_builder.h"
#include "builders/inverse_head_and_shoulders_builder.h"
#include "builders/hmm_builder.h"
#include "builders/gmm_builder.h"
#include "builders/linear_model_builder.h"
#include "builders/lightgbm_builder.h"
#include "builders/line_builder.h"
#include "builders/h_line_builder.h"
#include "builders/vwap_builder.h"
#include "builders/column_builder.h"
#include "builders/ao_builder.h"
#include "builders/qstick_builder.h"
#include "builders/bb_percent_b_builder.h"
#include "builders/psar_builder.h"
#include "builders/panel_line_builder.h"
#include "builders/panel_line_percent_builder.h"
#include "builders/rsi_builder.h"
#include "builders/cci_builder.h"
#include "builders/atr_builder.h"
#include "builders/sentiment_builder.h"

namespace epoch_script::chart_metadata::plot_kinds {

PlotKindBuilderRegistry::PlotKindBuilderRegistry() {
  InitializeBuilders();
}

PlotKindBuilderRegistry& PlotKindBuilderRegistry::Instance() {
  static PlotKindBuilderRegistry instance;
  return instance;
}

void PlotKindBuilderRegistry::Register(
  epoch_core::TransformPlotKind plotKind,
  std::unique_ptr<IPlotKindBuilder> builder
) {
  builders_[plotKind] = std::move(builder);
}

const IPlotKindBuilder& PlotKindBuilderRegistry::GetBuilder(
  epoch_core::TransformPlotKind plotKind
) const {
  auto it = builders_.find(plotKind);
  if (it == builders_.end()) {
    throw std::runtime_error(
      "PlotKind not registered: " +
      epoch_core::TransformPlotKindWrapper::ToString(plotKind)
    );
  }
  return *it->second;
}

void PlotKindBuilderRegistry::InitializeBuilders() {
  using PK = epoch_core::TransformPlotKind;

  // Multi-line indicators
  Register(PK::macd, std::make_unique<MACDBuilder>());
  Register(PK::aroon, std::make_unique<AroonBuilder>());
  Register(PK::stoch, std::make_unique<StochBuilder>());
  Register(PK::fisher, std::make_unique<FisherBuilder>());
  Register(PK::qqe, std::make_unique<QQEBuilder>());
  Register(PK::elders, std::make_unique<EldersBuilder>());
  Register(PK::fosc, std::make_unique<FOscBuilder>());
  Register(PK::vortex, std::make_unique<VortexBuilder>());

  // Bands
  Register(PK::bbands, std::make_unique<BbandsBuilder>());
  Register(PK::bb_percent_b, std::make_unique<BBPercentBBuilder>());

  // Complex indicators
  Register(PK::ichimoku, std::make_unique<IchimokuBuilder>());
  Register(PK::chande_kroll_stop, std::make_unique<ChandeKrollStopBuilder>());
  Register(PK::pivot_point_sr, std::make_unique<PivotPointSRBuilder>());
  Register(PK::previous_high_low, std::make_unique<PreviousHighLowBuilder>());
  Register(PK::retracements, std::make_unique<RetracementsBuilder>());
  Register(PK::gap, std::make_unique<GapBuilder>());
  Register(PK::shl, std::make_unique<SHLBuilder>());
  Register(PK::bos_choch, std::make_unique<BosChochBuilder>());
  Register(PK::order_blocks, std::make_unique<OrderBlocksBuilder>());
  Register(PK::fvg, std::make_unique<FVGBuilder>());
  Register(PK::liquidity, std::make_unique<LiquidityBuilder>());
  Register(PK::sessions, std::make_unique<SessionsBuilder>());
  Register(PK::pivot_point_detector, std::make_unique<PivotPointDetectorBuilder>());
  Register(PK::hmm, std::make_unique<HMMBuilder>());
  Register(PK::gmm, std::make_unique<GMMBuilder>());
  Register(PK::linear_model, std::make_unique<LinearModelBuilder>());
  Register(PK::lightgbm, std::make_unique<LightGBMBuilder>());

  // Pattern detection
  Register(PK::flag_pattern, std::make_unique<FlagPatternBuilder>());
  Register(PK::pennant_pattern, std::make_unique<PennantPatternBuilder>());
  Register(PK::triangle_patterns, std::make_unique<TrianglePatternsBuilder>());
  Register(PK::consolidation_box, std::make_unique<ConsolidationBoxBuilder>());
  Register(PK::double_top_bottom, std::make_unique<DoubleTopBottomBuilder>());
  Register(PK::head_and_shoulders, std::make_unique<HeadAndShouldersBuilder>());
  Register(PK::inverse_head_and_shoulders, std::make_unique<InverseHeadAndShouldersBuilder>());

  // Special purpose
  Register(PK::trade_signal, std::make_unique<TradeSignalBuilder>());
  Register(PK::flag, std::make_unique<FlagBuilder>());
  Register(PK::zone, std::make_unique<ZoneBuilder>());
  Register(PK::close_line, std::make_unique<CloseLineBuilder>());

  // Single-value indicators (all use SingleValueBuilder)
  Register(PK::line, std::make_unique<LineBuilder>());
  Register(PK::h_line, std::make_unique<HLineBuilder>());
  Register(PK::vwap, std::make_unique<VWAPBuilder>());
  Register(PK::column, std::make_unique<ColumnBuilder>());
  Register(PK::ao, std::make_unique<AOBuilder>());
  Register(PK::qstick, std::make_unique<QStickBuilder>());
  Register(PK::psar, std::make_unique<PSARBuilder>());
  Register(PK::panel_line, std::make_unique<PanelLineBuilder>());
  Register(PK::panel_line_percent, std::make_unique<PanelLinePercentBuilder>());
  Register(PK::rsi, std::make_unique<RSIBuilder>());
  Register(PK::cci, std::make_unique<CCIBuilder>());
  Register(PK::atr, std::make_unique<ATRBuilder>());

  // ML/AI indicators
  Register(PK::sentiment, std::make_unique<SentimentBuilder>());
}

} // namespace epoch_script::chart_metadata::plot_kinds

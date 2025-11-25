#include "alpaca_websocket_manager.h"
#include "epoch_core/macros.h"
#include "epoch_core/ranges_to.h"
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <algorithm>
#include <arrow/compute/api.h>
#include <drogon/HttpAppFramework.h>
#include <format>
#include <glaze/json/generic.hpp>
#include <limits>
#include <ranges>
#include <spdlog/spdlog.h>
#include <utility>

#include "epoch_frame/aliases.h"

using namespace std::chrono_literals;

namespace epoch_script::data {
namespace asset = data_sdk::asset;

constexpr auto kAlpacaBaseUrl = "wss://stream.data.alpaca.markets";
constexpr auto kStocksPath = "/v2/{}";
constexpr auto kCryptoPath = "/v1beta3/crypto/us";
constexpr auto kTestPath = "/v2/test";

std::string AlpacaWebSocketManager::GetPath() const {
  if (m_options.testing) {
    SPDLOG_DEBUG("Using TEST endpoint {}", kTestPath);
    return std::string{kTestPath};
  }

  SPDLOG_DEBUG("Selecting endpoint for asset‑class {}",
               AssetClassWrapper::ToString(m_options.assetClass));

  switch (m_options.assetClass) {
  case AssetClass::Stocks:
    return std::format(kStocksPath, m_options.feed);
  case AssetClass::Crypto:
    return std::string{kCryptoPath};
  default:
    break;
  }
  ThrowExceptionFromStream(
      "AlpacaWebSocketManager::GetPath: Invalid asset class: "
      << AssetClassWrapper::ToString(m_options.assetClass));
}

AlpacaWebSocketManager::AlpacaWebSocketManager(
    const AlpacaWebSocketManagerOptions &options)
    : m_options(options),
      m_client(drogon::WebSocketClient::newWebSocketClient(kAlpacaBaseUrl)) {
  m_client->setMessageHandler([this](const std::string &message,
                                     const drogon::WebSocketClientPtr &wsPtr,
                                     const drogon::WebSocketMessageType &type) {
    if (type == drogon::WebSocketMessageType::Text) {
      if (handleControlMessage(message, wsPtr)) {
        return;
      }
      drogon::app().getLoop()->queueInLoop([this, message] {
        try {
          ParseAndDispatch(message);
        } catch (std::exception const &ex) {
          SPDLOG_ERROR("Parse error: {}. Message: {}", ex.what(), message);
          // Re-throw to properly propagate the error
          throw;
        }
      });
    }

    SPDLOG_DEBUG("new message ({})",
                 type == drogon::WebSocketMessageType::Text     ? "Text"
                 : type == drogon::WebSocketMessageType::Binary ? "Binary"
                 : type == drogon::WebSocketMessageType::Ping   ? "Ping"
                 : type == drogon::WebSocketMessageType::Pong   ? "Pong"
                                                                : "Close");
  });

  m_client->setConnectionClosedHandler(
      [this](const drogon::WebSocketClientPtr &) {
        SPDLOG_INFO("WebSocket connection closed!");
        onClosed();
      });
}

AlpacaWebSocketManager::~AlpacaWebSocketManager() { Disconnect(); }

void AlpacaWebSocketManager::ParseAndDispatch(const std::string &raw) const {
  BarList bars;
  auto parsed = glz::read_json<glz::generic>(raw);
  if (!parsed || !parsed->is_array()) {
    SPDLOG_WARN("Data frame is not a JSON array: {}", raw);
    return;
  }
  for (auto &obj : parsed->get_array()) {
    if (obj["T"].get_string() != "b")
      continue;
    std::string symbol;
    try {
      symbol = std::string(obj["S"].get_string());
    } catch (const std::exception& exp) {
      // Try alternate field name (lowercase 's' instead of uppercase 'S')
      SPDLOG_DEBUG("Failed to get symbol from 'S' field, trying 's' field: {}", exp.what());
      try {
        symbol = std::string(obj["s"].get_string());
      } catch (const std::exception& exp2) {
        throw std::runtime_error(std::format("Failed to extract symbol from bar data. Neither 'S' nor 's' field found: {} / {}", exp.what(), exp2.what()));
      }
    } catch (...) {
      // Try alternate field name (lowercase 's' instead of uppercase 'S')
      SPDLOG_DEBUG("Failed to get symbol from 'S' field (unknown error), trying 's' field");
      symbol = std::string(obj["s"].get_string());
    }
    const double o = obj["o"].get_number();
    const double h = obj["h"].get_number();
    const double l = obj["l"].get_number();
    const double c = obj["c"].get_number();
    const double v = obj["v"].get_number();
    auto t_utc = std::string(obj["t"].get_string());
    if (t_utc.ends_with('Z'))
      t_utc.pop_back();
    auto ts = arrow::compute::Strptime(
        arrow::MakeScalar(t_utc),
        arrow::compute::StrptimeOptions{"%Y-%m-%dT%H:%M:%S",
                                        arrow::TimeUnit::NANO});
    if (!ts.ok())
      throw std::runtime_error("bad timestamp");
    auto timestamp = ts->scalar_as<arrow::TimestampScalar>().value;
    bars.emplace_back(BarMessage{symbol, o, h, l, c, v, timestamp});
  }
  SPDLOG_DEBUG("Parsed {} bar(s) from message: {}", bars.size(), raw);
  if (!bars.empty())
    SPDLOG_INFO("Sending {} bar(s) to subscribers.", bars.size());
  try {
    m_newMessageSignal(bars);
  } catch (std::exception const &ex) {
    SPDLOG_ERROR("Signal dispatch error: {}. Failed to notify subscribers of {} bar(s)", ex.what(), bars.size());
    // Re-throw to properly propagate the error
    throw;
  }
}

void AlpacaWebSocketManager::Connect() {
  SPDLOG_INFO("Connecting to {} …", GetPath());

  if (m_state != ConnectionState::Idle && m_state != ConnectionState::Closing)
    return;

  m_state = ConnectionState::Connecting;

  const auto req = drogon::HttpRequest::newHttpRequest();
  req->setPath(GetPath());

  m_client->connectToServer(
      req, [this](drogon::ReqResult r, const drogon::HttpResponsePtr &,
                  const drogon::WebSocketClientPtr &wsPtr) {
        if (r != drogon::ReqResult::Ok) {
          SPDLOG_ERROR("Failed to establish WebSocket connection!");
          wsPtr->stop();
          return;
        }
        SPDLOG_INFO("WebSocket connected!");
        m_state = ConnectionState::Authenticating;

        AuthRequest authenticator{.key = m_options.key,
                                  .secret = m_options.secret};

        auto json_str = glz::write_json(authenticator);
        if (json_str) {
          wsPtr->getConnection()->send(*json_str);
        } else {
          SPDLOG_ERROR("Failed to serialize auth request!");
          wsPtr->stop();
        }
      });
}

void AlpacaWebSocketManager::Disconnect() {
  SPDLOG_INFO("Manual disconnect requested.");
  m_manualCloseRequested = true;
  m_state = ConnectionState::Closing;
  m_client->stop();
}

void AlpacaWebSocketManager::Subscribe(const asset::AssetHashSet &assets) {
  SPDLOG_DEBUG("Subscribe() called with {} symbol(s).", assets.size());
  if (assets.empty())
    return;
  m_subQueue.push(assets);

  if (m_state == ConnectionState::Streaming) {
    flushSubscriptions();
  }
}

std::string AlpacaWebSocketManager::GetSymbol(const asset::Asset &asset) {
  if (asset.GetAssetClass() == AssetClass::Crypto) {
    auto [base, quote] = asset.GetCurrencyPair();
    return std::format("{}/{}", base.ToString(), quote.ToString());
  }
  return asset.GetSymbolStr();
}

bool AlpacaWebSocketManager::ValidateAssets(
    const asset::AssetHashSet &assets) const {
  const bool valid =
      std::ranges::all_of(assets, [this](const asset::Asset &asset) {
        return asset.GetAssetClass() == m_options.assetClass;
      });
  if (!valid) {
    SPDLOG_ERROR("All assets must be of the same asset class: {}",
                 AssetClassWrapper::ToLongFormString(m_options.assetClass));
  }
  return valid;
}

void AlpacaWebSocketManager::CompleteSubscriptionRequest(
    SubscriptionRequest const &req) const {
  auto json_str = glz::write_json(req);
  if (json_str.has_value()) {
    SPDLOG_INFO("Sending subscribe request: {}", json_str.value());
    m_client->getConnection()->send(json_str.value());
  } else {
    SPDLOG_ERROR("Failed to serialize subscribe request!");
  }
}

void AlpacaWebSocketManager::flushSubscriptions() {
  drogon::app().getLoop()->queueInLoop([this] {
    if (m_state != ConnectionState::Streaming)
      return;

    size_t flushed = 0;
    asset::AssetHashSet assets;
    while (m_subQueue.try_pop(assets)) {
      if (!ValidateAssets(assets))
        continue;

      SubscriptionRequest req;
      req.bars.reserve(assets.size());
      std::ranges::transform(assets, std::back_inserter(req.bars), GetSymbol);
      SPDLOG_DEBUG("Flushing subscribe request: {}",
                   req.bars | std::views::join_with(',') | ranges::to_string_v);
      CompleteSubscriptionRequest(req);
      ++flushed;
    }
    if (flushed) {
      SPDLOG_INFO("Flushed {} pending subscribe request(s).", flushed);
    }
  });
}

bool AlpacaWebSocketManager::handleControlMessage(
    const std::string &msg, const drogon::WebSocketClientPtr &wsPtr) {
  // Parse `[ { … } ]` array coming from Alpaca
  auto parsed = glz::read_json<glz::generic>(msg);
  if (!parsed) {
    SPDLOG_WARN("Control message is not valid JSON: {}\nErr: {}", msg,
                glz::format_error(parsed));
    return false;
  }

  if (!parsed->is_array() || parsed->get_array().empty()) {
    return false; // not a control frame
  }
  const auto &obj = parsed->get_array().front();

  const auto type = obj["T"].get_string();

  /* ------------------------------------------------------------------ *
   * SUCCESS messages                                                   *
   * ------------------------------------------------------------------ */
  if (type == "success") {
    const auto reason = obj["msg"].get_string();
    SPDLOG_INFO("SUCCESS: {}", reason);

    if (reason == "authenticated") { // we are ready to stream
      m_state = ConnectionState::Streaming;
      m_reconnectAttempts = 0;
      flushSubscriptions();
    } else if (reason == "connected") {
      // nothing else to do
    }
    return true;
  }

  /* ------------------------------------------------------------------ *
   * SUBSCRIPTION snapshot                                              *
   * ------------------------------------------------------------------ */
  if (type == "subscription") {
    SPDLOG_INFO("Current subscription set: {}", msg);
    return true; // already JSON → print raw
  }

  /* ------------------------------------------------------------------ *
   * ERROR messages                                                     *
   * ------------------------------------------------------------------ */
  if (type == "error") {
    auto code = static_cast<size_t>(obj["code"].get_number());
    const auto text = obj["msg"].get_string();
    SPDLOG_ERROR("SERVER ERROR ({}): {}", code, text);

    switch (code) {
    case 401: // not authenticated
    case 402: // auth failed
    case 403: // already authenticated
    case 404: // auth timeout
    case 406: // connection limit exceeded
      m_state = ConnectionState::Closing;
      wsPtr->stop(); // triggers onClosed()
      break;

    case 405: // symbol limit exceeded
      SPDLOG_WARN(
          "epoch_script::Symbol‑limit exceeded – request ignored.");
      break;

    case 409: // insufficient subscription
      SPDLOG_WARN("Insufficient subscription: {}", text);
      break;

    default:
      SPDLOG_WARN("Unhandled error code {}, continuing …", code);
      break;
    }
    return true;
  }

  /* ------------------------------------------------------------------ *
   * Anything else is not a control message                             *
   * ------------------------------------------------------------------ */
  return false;
}

void AlpacaWebSocketManager::onClosed() {
  m_state = ConnectionState::Idle;

  if (m_manualCloseRequested) {
    m_manualCloseRequested = false;
    return;
  }

  const std::uint32_t delay =
      std::min<std::uint32_t>(30, 1u << std::min(10u, ++m_reconnectAttempts));

  SPDLOG_INFO("Connection lost. attempt={} scheduling reconnect in {}s",
              m_reconnectAttempts.load(), delay);

  drogon::app().getLoop()->runAfter(delay, [this] { this->Connect(); });
}

} // namespace epoch_script::data

#pragma once
#include "iintermediate_storage.h"
#include "thread_safe_logger.h"

namespace epoch_script::runtime {

struct ExecutionContext {
  std::unique_ptr<IIntermediateStorage> cache;
  ILoggerPtr logger;
};

} // namespace epoch_script::runtime

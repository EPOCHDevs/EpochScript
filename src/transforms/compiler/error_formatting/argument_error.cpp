#include "argument_error.h"
#include <sstream>

namespace epoch_script {
namespace error_formatting {

std::string ArgumentCountError::Format(int line, int col) const {
    std::ostringstream oss;

    oss << "Too many positional inputs for '" << node_id_ << "'\n";
    oss << Indent("Component: " + component_name_ + "()") << "\n";

    // Expected arguments
    oss << Indent("Expected: " + std::to_string(expected_count_) + " argument(s)");
    if (!expected_names_.empty()) {
        oss << " " << FormatListInBrackets(expected_names_);
    }
    oss << "\n";

    // Received arguments
    oss << Indent("Received: " + std::to_string(received_count_) + " argument(s)");
    if (!received_args_.empty()) {
        std::vector<std::string> arg_sources;
        for (const auto& arg : received_args_) {
            arg_sources.push_back(arg.GetColumnIdentifier());
        }
        oss << " " << FormatListInBrackets(arg_sources);
    }

    return AddLocationInfo(oss.str(), line, col);
}

std::string UnknownInputError::Format(int line, int col) const {
    std::ostringstream oss;

    oss << "Unknown input '" << input_name_ << "' for '" << component_name_ << "()'\n";
    oss << Indent("Node: " + node_id_) << "\n";

    if (!valid_inputs_.empty()) {
        oss << Indent("Valid inputs: " + FormatList(valid_inputs_));
    } else {
        oss << Indent("This component has no inputs");
    }

    return AddLocationInfo(oss.str(), line, col);
}

} // namespace error_formatting
} // namespace epoch_script
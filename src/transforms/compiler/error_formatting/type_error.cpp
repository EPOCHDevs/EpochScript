#include "type_error.h"
#include <sstream>

namespace epoch_script {
namespace error_formatting {

std::string TypeMismatchError::Format(int line, int col) const {
    std::ostringstream oss;

    oss << "Type error calling '" << component_name_ << "()'\n";

    // Describe which argument has the error
    if (kind_ == ArgumentKind::Keyword) {
        oss << Indent("Keyword argument '" + arg_identifier_ + "':");
    } else {
        oss << Indent("Positional argument " + arg_identifier_ + " ('" + arg_identifier_ + "'):");
    }
    oss << "\n";

    // Expected vs received
    oss << Indent("Expected type: " + TypeChecker::DataTypeToString(expected_type_), 4) << "\n";
    oss << Indent("Received type: " + TypeChecker::DataTypeToString(received_type_), 4) << "\n";
    oss << Indent("Source: " + source_handle_.GetColumnIdentifier(), 4);

    return AddLocationInfo(oss.str(), line, col);
}

std::string BinaryOpTypeError::Format(int line, int col) const {
    std::ostringstream oss;

    oss << "Type error in " << (operand_ == Operand::Left ? "left" : "right")
        << " operand of '" << operator_name_ << "' operation\n";

    oss << Indent("Operand name: " + operand_name_) << "\n";
    oss << Indent("Expected type: " + TypeChecker::DataTypeToString(expected_type_)) << "\n";
    oss << Indent("Received type: " + TypeChecker::DataTypeToString(received_type_)) << "\n";
    oss << Indent("Source: " + source_handle_.GetColumnIdentifier());

    return AddLocationInfo(oss.str(), line, col);
}

} // namespace error_formatting
} // namespace epoch_script
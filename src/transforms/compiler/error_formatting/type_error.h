#pragma once

#include "error_formatter.h"
#include "../type_checker.h"
#include "../compilation_context.h"

namespace epoch_script {
namespace error_formatting {

/**
 * Formats type mismatch errors for function arguments.
 */
class TypeMismatchError : public ErrorFormatter {
public:
    enum class ArgumentKind {
        Positional,
        Keyword
    };

    TypeMismatchError(
        const std::string& component_name,
        ArgumentKind kind,
        const std::string& arg_identifier,  // "SLOT0" or "data" or "1" (for positional arg number)
        DataType expected_type,
        DataType received_type,
        const strategy::InputValue& source_handle
    ) : component_name_(component_name),
        kind_(kind),
        arg_identifier_(arg_identifier),
        expected_type_(expected_type),
        received_type_(received_type),
        source_handle_(source_handle) {}

    std::string Format(int line = -1, int col = -1) const override;

private:
    std::string component_name_;
    ArgumentKind kind_;
    std::string arg_identifier_;
    DataType expected_type_;
    DataType received_type_;
    strategy::InputValue source_handle_;
};

/**
 * Formats type errors in binary operations.
 */
class BinaryOpTypeError : public ErrorFormatter {
public:
    enum class Operand {
        Left,
        Right
    };

    BinaryOpTypeError(
        const std::string& operator_name,
        Operand operand,
        const std::string& operand_name,
        DataType expected_type,
        DataType received_type,
        const strategy::InputValue& source_handle
    ) : operator_name_(operator_name),
        operand_(operand),
        operand_name_(operand_name),
        expected_type_(expected_type),
        received_type_(received_type),
        source_handle_(source_handle) {}

    std::string Format(int line = -1, int col = -1) const override;

private:
    std::string operator_name_;
    Operand operand_;
    std::string operand_name_;
    DataType expected_type_;
    DataType received_type_;
    strategy::InputValue source_handle_;
};

} // namespace error_formatting
} // namespace epoch_script
#pragma once

#include "error_formatter.h"
#include "../compilation_context.h"
#include <vector>

namespace epoch_script {
namespace error_formatting {

/**
 * Formats errors related to function/component argument mismatches.
 */
class ArgumentCountError : public ErrorFormatter {
public:
    ArgumentCountError(
        const std::string& node_id,
        const std::string& component_name,
        size_t expected_count,
        size_t received_count,
        const std::vector<std::string>& expected_names,
        const std::vector<strategy::InputValue>& received_args
    ) : node_id_(node_id),
        component_name_(component_name),
        expected_count_(expected_count),
        received_count_(received_count),
        expected_names_(expected_names),
        received_args_(received_args) {}

    std::string Format(int line = -1, int col = -1) const override;

private:
    std::string node_id_;
    std::string component_name_;
    size_t expected_count_;
    size_t received_count_;
    std::vector<std::string> expected_names_;
    std::vector<strategy::InputValue> received_args_;
};

/**
 * Formats errors for unknown input handles.
 */
class UnknownInputError : public ErrorFormatter {
public:
    UnknownInputError(
        const std::string& input_name,
        const std::string& node_id,
        const std::string& component_name,
        const std::vector<std::string>& valid_inputs
    ) : input_name_(input_name),
        node_id_(node_id),
        component_name_(component_name),
        valid_inputs_(valid_inputs) {}

    std::string Format(int line = -1, int col = -1) const override;

private:
    std::string input_name_;
    std::string node_id_;
    std::string component_name_;
    std::vector<std::string> valid_inputs_;
};

} // namespace error_formatting
} // namespace epoch_script
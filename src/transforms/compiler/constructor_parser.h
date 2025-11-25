//
// Created by Claude Code
// EpochScript Constructor Parser
//
// Parses constructor calls and feed chains from AST.
// Handles both named constructors with options and feed operator chains.
//

#pragma once

#include "compilation_context.h"
#include "parser/ast_nodes.h"
#include <epoch_script/core/metadata_options.h>
#include <epoch_script/transforms/core/metadata.h>
#include <unordered_map>
#include <vector>

namespace epoch_script
{

    // Forward declaration
    class ExpressionCompiler;

    // Result of parsing a constructor call chain
    struct ConstructorParseResult
    {
        std::string ctor_name;
        std::unordered_map<std::string, epoch_script::MetaDataOptionDefinition::T> ctor_kwargs;
        std::vector<std::pair<std::vector<strategy::InputValue>, std::unordered_map<std::string, strategy::InputValue>>> feed_steps;
    };

    class ConstructorParser
    {
    public:
        ConstructorParser(CompilationContext& context, ExpressionCompiler& expr_compiler)
            : context_(context), expr_compiler_(expr_compiler) {}

        // Check if an expression is a constructor call
        bool IsConstructorCall(const Expr& expr);

        // Parse constructor and feed chain from a Call node
        ConstructorParseResult ParseConstructorAndFeeds(const Call& call);

        // Parse a literal or primitive value for use as an option
        epoch_script::MetaDataOptionDefinition::T ParseLiteralOrPrimitive(
            const Expr& expr,
            const epoch_script::MetaDataOption& meta_option,
            const epoch_script::transforms::TransformsMetaData& comp_meta);

    private:
        CompilationContext& context_;
        ExpressionCompiler& expr_compiler_;

        // Error reporting helper
        [[noreturn]] void ThrowError(const std::string& msg, int line = 0, int col = 0);

        // Custom type constructor parsers
        // These correspond to types in MetaDataOptionDefinition::T variant
        epoch_frame::Time ParseTimeConstructor(const Call& call);
        epoch_script::EventMarkerSchema ParseEventMarkerSchemaConstructor(const Call& call);
        epoch_script::SqlStatement ParseSqlStatementConstructor(const Call& call);
        epoch_script::TableReportSchema ParseTableReportSchemaConstructor(const Call& call);
        epoch_script::CardColumnSchema ParseCardColumnSchemaConstructor(const Call& call);
        // Note: SessionRange and TimeFrame are handled as special parameters, not regular options

        // Helper to convert Call kwargs to glz::generic for deserialization
        glz::generic CallKwargsToGeneric(const Call& call);
    };

} // namespace epoch_script

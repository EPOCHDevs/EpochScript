//
// Created by Claude Code
// EpochScript Expression Compiler
//
// Compiles AST expressions into ValueHandles.
// Handles operators, function calls, literals, attributes, and subscripts.
//

#pragma once

#include "compilation_context.h"
#include "type_checker.h"
#include "constructor_parser.h"
#include "parser/ast_nodes.h"

namespace epoch_script
{

    // Forward declarations
    class ConstructorParser;
    class OptionValidator;
    class SpecialParameterHandler;

    class ExpressionCompiler
    {
    public:
        ExpressionCompiler(
            CompilationContext& context,
            TypeChecker& type_checker,
            OptionValidator& option_validator,
            SpecialParameterHandler& special_param_handler)
            : context_(context),
              type_checker_(type_checker),
              option_validator_(option_validator),
              special_param_handler_(special_param_handler),
              constructor_parser_(nullptr) {}

        // Set constructor parser (circular dependency resolved via setter)
        void SetConstructorParser(ConstructorParser* parser) { constructor_parser_ = parser; }

        // Visit expression and return value handle
        strategy::InputValue VisitExpr(const Expr& expr);

        // Visit specific expression types
        strategy::InputValue VisitCall(const Call& call);
        strategy::InputValue VisitAttribute(const Attribute& attr);
        strategy::InputValue VisitName(const Name& name);
        strategy::InputValue VisitConstant(const Constant& constant);
        strategy::InputValue VisitBinOp(const BinOp& binOp);
        strategy::InputValue VisitUnaryOp(const UnaryOp& unaryOp);
        strategy::InputValue VisitCompare(const Compare& compare);
        strategy::InputValue VisitBoolOp(const BoolOp& boolOp);
        strategy::InputValue VisitIfExp(const IfExp& ifExp);
        strategy::InputValue VisitSubscript(const Subscript& subscript);

        // Materialize literal nodes
        strategy::InputValue MaterializeNumber(double value);
        strategy::InputValue MaterializeBoolean(bool value);
        strategy::InputValue MaterializeText(const std::string& value);
        strategy::InputValue MaterializeNull();

    private:
        CompilationContext& context_;
        TypeChecker& type_checker_;
        OptionValidator& option_validator_;
        SpecialParameterHandler& special_param_handler_;
        ConstructorParser* constructor_parser_;

        // Attribute resolution helpers
        std::pair<std::string, std::string> AttributeToTuple(const Attribute& attr);
        strategy::InputValue ResolveHandle(const std::string& var, const std::string& handle);

        // Helper utilities
        std::string UniqueNodeId(const std::string& base);
        std::string JoinId(const std::string& node_id, const std::string& handle);
        void WireInputs(
            const std::string& target_node_id,
            const std::string& component_name,
            const std::vector<strategy::InputValue>& args,
            const std::unordered_map<std::string, strategy::InputValue>& kwargs);

        // Type specialization helpers
        std::string DetermineBooleanSelectVariant(DataType true_type, DataType false_type);
        std::string DetermineLagVariant(DataType input_type);
        std::string DetermineNullVariant(DataType expected_type);

        // Error reporting helper
        [[noreturn]] void ThrowError(const std::string& msg, int line = 0, int col = 0);
    };

} // namespace epoch_script

//
// Created by Claude Code
// EpochScript Python Parser Implementation
//

#include "python_parser.h"
#include <tree_sitter/api.h>
#include <functional>
#include <regex>

// Declare the tree-sitter-python language function
extern "C" {
    const TSLanguage *tree_sitter_python();
}

namespace epoch_script {

PythonParser::PythonParser() {
    // Initialize parser with Python language
    parser_.emplace(ts::Language{tree_sitter_python()});
}

ModulePtr PythonParser::parse(const std::string& source) {
    // Preprocess source: convert backticks to double quotes
    // Backticks are not valid Python string delimiters and cause syntax errors
    // std::string preprocessed_source = preprocessSource(source);

    ts::Tree tree = parser_->parseString(source);

    ts::Node root = tree.getRootNode();

    // Check for parse errors
    if (root.hasError()) {
        throw PythonParseError("Syntax error in Python source", 0, 0);
    }

    return parseModule(root, source);
}

std::string PythonParser::preprocessSource(const std::string& source) {
    std::string result = source;

    // Fix 1: Replace backticks with double quotes
    // Pattern: `some string content` -> "some string content"
    // Avoid backticks with nested quotes to prevent creating invalid syntax
    std::regex backtick_pattern(R"(`([^`"']*)`)", std::regex_constants::ECMAScript);
    result = std::regex_replace(result, backtick_pattern, "\"$1\"");

    // Fix 2a: Special case - move trailing asterisk(s) inside string before closing quote
    // Pattern: "content'* -> "content*" (preserves content, makes valid syntax)
    std::regex asterisk_inside(R"("([^"']*)'(\*+))", std::regex_constants::ECMAScript);
    result = std::regex_replace(result, asterisk_inside, "\"$1$2\"");

    // Fix 2b: Fix mismatched quotes - opening double quote with closing single quote
    // Pattern: "...' followed by ), ], }, or comma -> "..."
    // Combined pattern to handle all common closing contexts
    std::regex mismatch_pattern(R"("([^"']*)'([),\]\}]))", std::regex_constants::ECMAScript);
    result = std::regex_replace(result, mismatch_pattern, "\"$1\"$2");

    return result;
}

// Helper functions
std::string PythonParser::getNodeText(const ts::Node& node, std::string_view source) {
    return std::string{node.getSourceRange(source)};
}

void PythonParser::throwError(const std::string& msg, const ts::Node& node) {
    auto range = node.getPointRange();
    throw PythonParseError(msg, range.start.row + 1, range.start.column + 1);
}

// Module parsing
ModulePtr PythonParser::parseModule(const ts::Node& node, std::string_view source) {
    auto module = std::make_unique<Module>();

    uint32_t childCount = node.getNumChildren();
    for (uint32_t i = 0; i < childCount; ++i) {
        ts::Node child = node.getChild(i);
        std::string type{child.getType()};

        // Skip comments and whitespace
        if (type == "comment" || type == "\n") {
            continue;
        }

        // In tree-sitter v0.23.6, module children might be "assignment" directly or wrapped in other nodes
        // Try to parse as statement, or handle assignment if it appears at module level
        if (type == "assignment") {
            if (auto stmt = parseAssignment(child, source)) {
                module->body.push_back(std::move(stmt));
            }
        } else if (auto stmt = parseStatement(child, source)) {
            module->body.push_back(std::move(stmt));
        }
    }

    return module;
}

// Statement parsing
StmtPtr PythonParser::parseStatement(const ts::Node& node, std::string_view source) {
    std::string type{node.getType()};

    if (type == "expression_statement") {
        return parseExprStmt(node, source);
    } else if (type == "assignment") {
        return parseAssignment(node, source);
    } else if (type == "comment") {
        return nullptr;  // Skip comments
    }

    // Disallowed constructs
    if (type == "import_statement" || type == "import_from_statement" ||
        type == "function_definition" || type == "class_definition" ||
        type == "if_statement" || type == "for_statement" ||
        type == "while_statement" || type == "with_statement") {
        throwError("Disallowed construct in algorithm section: " + type, node);
    }

    return nullptr;
}

StmtPtr PythonParser::parseExprStmt(const ts::Node& node, std::string_view source) {
    ts::Node exprNode = node.getChild(0);  // First child is the expression

    // Check if the child is actually an assignment (tree-sitter v0.23.6 behavior)
    std::string childType{exprNode.getType()};
    if (childType == "assignment") {
        return parseAssignment(exprNode, source);
    }

    auto expr = parseExpression(exprNode, source);

    auto stmt = std::make_unique<ExprStmt>(std::move(expr));
    auto range = node.getPointRange();
    stmt->lineno = range.start.row + 1;
    stmt->col_offset = range.start.column + 1;

    return stmt;
}

StmtPtr PythonParser::parseAssignment(const ts::Node& node, std::string_view source) {
    ts::Node leftNode = node.getChildByFieldName("left");
    ts::Node rightNode = node.getChildByFieldName("right");

    if (leftNode.isNull() || rightNode.isNull()) {
        throwError("Invalid assignment: missing left or right", node);
    }

    auto value = parseExpression(rightNode, source);
    auto stmt = std::make_unique<Assign>(std::move(value));

    // Parse targets (can be tuple or single name)
    auto target = parseExpression(leftNode, source);
    stmt->targets.push_back(std::move(target));

    auto range = node.getPointRange();
    stmt->lineno = range.start.row + 1;
    stmt->col_offset = range.start.column + 1;

    return stmt;
}

// Expression parsing
ExprPtr PythonParser::parseExpression(const ts::Node& node, std::string_view source) {
    std::string type{node.getType()};

    if (type == "call") {
        return parseCall(node, source);
    } else if (type == "attribute") {
        return parseAttribute(node, source);
    } else if (type == "identifier") {
        return parseName(node, source);
    } else if (type == "integer" || type == "float" || type == "string" ||
               type == "true" || type == "false" || type == "none") {
        return parseConstant(node, source);
    } else if (type == "binary_operator") {
        return parseBinaryOp(node, source);
    } else if (type == "comparison_operator") {
        return parseCompare(node, source);
    } else if (type == "boolean_operator") {
        return parseBoolOp(node, source);
    } else if (type == "unary_operator") {
        return parseUnaryOp(node, source);
    } else if (type == "not_operator") {
        // not_operator is a separate node type in tree-sitter for logical not
        return parseUnaryOp(node, source);
    } else if (type == "conditional_expression") {
        return parseIfExp(node, source);
    } else if (type == "subscript") {
        return parseSubscript(node, source);
    } else if (type == "tuple") {
        return parseTuple(node, source);
    } else if (type == "pattern_list") {
        // pattern_list is used for assignment targets (e.g., a, b, c = ...)
        // Handle it the same way as tuple
        return parseTuple(node, source);
    } else if (type == "list") {
        return parseList(node, source);
    } else if (type == "dictionary") {
        return parseDict(node, source);
    } else if (type == "parenthesized_expression") {
        // Unwrap parentheses
        ts::Node child = node.getChild(1);  // Skip opening paren
        return parseExpression(child, source);
    }

    throwError("Unsupported expression type: " + type, node);
    return nullptr;
}

ExprPtr PythonParser::parseName(const ts::Node& node, std::string_view source) {
    std::string name = getNodeText(node, source);
    return std::make_unique<Name>(name);
}

ExprPtr PythonParser::parseConstant(const ts::Node& node, std::string_view source) {
    std::string type{node.getType()};
    std::string text = getNodeText(node, source);

    if (type == "integer") {
        return std::make_unique<Constant>(std::stoi(text));
    } else if (type == "float") {
        return std::make_unique<Constant>(std::stod(text));
    } else if (type == "string") {
        // Remove quotes (handle both single/double and triple-quoted strings)
        std::string str = text;

        // Check for triple quotes (""" or ''')
        if (str.length() >= 6 &&
            ((str.substr(0, 3) == "\"\"\"" && str.substr(str.length() - 3) == "\"\"\"") ||
             (str.substr(0, 3) == "'''" && str.substr(str.length() - 3) == "'''"))) {
            str = str.substr(3, str.length() - 6);
        }
        // Check for single quotes (" or ')
        else if (str.length() >= 2 &&
                 ((str.front() == '"' && str.back() == '"') ||
                  (str.front() == '\'' && str.back() == '\''))) {
            str = str.substr(1, str.length() - 2);
        }

        return std::make_unique<Constant>(str);
    } else if (type == "true") {
        return std::make_unique<Constant>(true);
    } else if (type == "false") {
        return std::make_unique<Constant>(false);
    } else if (type == "none") {
        return std::make_unique<Constant>(std::monostate{});
    }

    throwError("Unknown constant type: " + type, node);
    return nullptr;
}

ExprPtr PythonParser::parseAttribute(const ts::Node& node, std::string_view source) {
    ts::Node objectNode = node.getChildByFieldName("object");
    ts::Node attributeNode = node.getChildByFieldName("attribute");

    if (objectNode.isNull() || attributeNode.isNull()) {
        throwError("Invalid attribute access", node);
    }

    auto object = parseExpression(objectNode, source);
    std::string attr = getNodeText(attributeNode, source);

    return std::make_unique<Attribute>(std::move(object), attr);
}

ExprPtr PythonParser::parseCall(const ts::Node& node, std::string_view source) {
    ts::Node funcNode = node.getChildByFieldName("function");
    ts::Node argsNode = node.getChildByFieldName("arguments");

    if (funcNode.isNull()) {
        throwError("Invalid call: missing function", node);
    }

    auto func = parseExpression(funcNode, source);
    auto call = std::make_unique<Call>(std::move(func));

    // Parse arguments
    if (!argsNode.isNull()) {
        uint32_t childCount = argsNode.getNumChildren();
        for (uint32_t i = 0; i < childCount; ++i) {
            ts::Node child = argsNode.getChild(i);
            std::string childType{child.getType()};

            if (childType == "(" || childType == ")" || childType == "," || childType == "comment") {
                continue;  // Skip delimiters and comments
            }

            if (childType == "keyword_argument") {
                ts::Node nameNode = child.getChildByFieldName("name");
                ts::Node valueNode = child.getChildByFieldName("value");

                std::string name = getNodeText(nameNode, source);
                auto value = parseExpression(valueNode, source);
                call->keywords.emplace_back(name, std::move(value));
            } else {
                // Positional argument
                auto arg = parseExpression(child, source);
                call->args.push_back(std::move(arg));
            }
        }
    }

    return call;
}

BinOpType PythonParser::parseBinOpType(const std::string& opText) {
    if (opText == "+") return BinOpType::Add;
    if (opText == "-") return BinOpType::Sub;
    if (opText == "*") return BinOpType::Mult;
    if (opText == "/") return BinOpType::Div;
    if (opText == "%") return BinOpType::Mod;
    if (opText == "**") return BinOpType::Pow;
    if (opText == "<") return BinOpType::Lt;
    if (opText == ">") return BinOpType::Gt;
    if (opText == "<=") return BinOpType::LtE;
    if (opText == ">=") return BinOpType::GtE;
    if (opText == "==") return BinOpType::Eq;
    if (opText == "!=") return BinOpType::NotEq;
    if (opText == "and") return BinOpType::And;
    if (opText == "&") return BinOpType::And;
    if (opText == "or") return BinOpType::Or;
    if (opText == "|") return BinOpType::Or;

    throw std::runtime_error("Unknown binary operator: " + opText);
}

UnaryOpType PythonParser::parseUnaryOpType(const std::string& opText) {
    if (opText == "not") return UnaryOpType::Not;
    if (opText == "!") return UnaryOpType::Not;
    if (opText == "-") return UnaryOpType::USub;
    if (opText == "+") return UnaryOpType::UAdd;

    throw std::runtime_error("Unknown unary operator: " + opText);
}

ExprPtr PythonParser::parseBinaryOp(const ts::Node& node, std::string_view source) {
    ts::Node leftNode = node.getChildByFieldName("left");
    ts::Node opNode = node.getChildByFieldName("operator");
    ts::Node rightNode = node.getChildByFieldName("right");

    auto left = parseExpression(leftNode, source);
    auto right = parseExpression(rightNode, source);
    std::string opText = getNodeText(opNode, source);
    BinOpType op = parseBinOpType(opText);

    return std::make_unique<BinOp>(op, std::move(left), std::move(right));
}

ExprPtr PythonParser::parseCompare(const ts::Node& node, std::string_view source) {
    // In tree-sitter-python v0.23.6, comparison_operator structure:
    // - Simple: uses "left" field for left expr, followed by operator and remaining expressions
    // - Chained: may have multiple operators and comparators as named children

    // Get left operand
    ts::Node leftNode = node.getChildByFieldName("left");
    if (leftNode.isNull()) {
        // Fallback: first child might be the left expression
        leftNode = node.getChild(0);
    }

    if (leftNode.isNull()) {
        throwError("Invalid comparison: missing left operand", node);
    }

    auto left = parseExpression(leftNode, source);
    auto compare = std::make_unique<Compare>(std::move(left));

    // Parse operators and comparators by iterating through children
    uint32_t childCount = node.getNumChildren();
    bool foundOperator = false;

    for (uint32_t i = 1; i < childCount; ++i) {  // Start at 1 to skip left node
        ts::Node child = node.getChild(i);
        if (child.isNull()) continue;

        std::string childType{child.getType()};

        // Check if this is an operator token
        if (childType == "<" || childType == ">" || childType == "<=" ||
            childType == ">=" || childType == "==" || childType == "!=") {
            std::string opText = getNodeText(child, source);
            compare->ops.push_back(parseBinOpType(opText));
            foundOperator = true;
        } else {
            // This should be a comparator expression (skip if it's the left we already processed)
            try {
                auto comparator = parseExpression(child, source);
                compare->comparators.push_back(std::move(comparator));
            } catch (...) {
                // Skip nodes that aren't valid expressions (like whitespace)
                continue;
            }
        }
    }

    if (!foundOperator || compare->comparators.empty()) {
        throwError("Invalid comparison: no operator or comparator found", node);
    }

    return compare;
}

ExprPtr PythonParser::parseBoolOp(const ts::Node& node, std::string_view source) {
    ts::Node leftNode = node.getChildByFieldName("left");
    ts::Node opNode = node.getChildByFieldName("operator");
    ts::Node rightNode = node.getChildByFieldName("right");

    std::string opText = getNodeText(opNode, source);
    BinOpType op = parseBinOpType(opText);

    auto boolOp = std::make_unique<BoolOp>(op);

    // Flatten chained boolean operations (like Python's ast module)
    // e.g., "a and b and c" becomes BoolOp([a, b, c]) instead of BoolOp(BoolOp(a, b), c)

    // Check if left is a BoolOp with the same operator - if so, flatten it
    auto leftExpr = parseExpression(leftNode, source);
    if (auto *leftBoolOp = dynamic_cast<BoolOp *>(leftExpr.get())) {
        if (leftBoolOp->op == op) {
            // Same operator - flatten by taking all its values
            for (auto &val : leftBoolOp->values) {
                boolOp->values.push_back(std::move(val));
            }
        } else {
            // Different operator - keep as nested
            boolOp->values.push_back(std::move(leftExpr));
        }
    } else {
        boolOp->values.push_back(std::move(leftExpr));
    }

    // Check if right is a BoolOp with the same operator - if so, flatten it
    auto rightExpr = parseExpression(rightNode, source);
    if (auto *rightBoolOp = dynamic_cast<BoolOp *>(rightExpr.get())) {
        if (rightBoolOp->op == op) {
            // Same operator - flatten by taking all its values
            for (auto &val : rightBoolOp->values) {
                boolOp->values.push_back(std::move(val));
            }
        } else {
            // Different operator - keep as nested
            boolOp->values.push_back(std::move(rightExpr));
        }
    } else {
        boolOp->values.push_back(std::move(rightExpr));
    }

    return boolOp;
}

ExprPtr PythonParser::parseUnaryOp(const ts::Node& node, std::string_view source) {
    std::string nodeType{node.getType()};

    // not_operator has different structure: "not" keyword is first child, argument is second
    if (nodeType == "not_operator") {
        // For not_operator: child[0] is "not" keyword, child[1] is argument
        if (node.getNumChildren() < 2) {
            throwError("Invalid not_operator: missing argument", node);
        }

        ts::Node operandNode = node.getChild(1);
        auto operand = parseExpression(operandNode, source);
        return std::make_unique<UnaryOp>(UnaryOpType::Not, std::move(operand));
    }

    // For unary_operator: use field names
    ts::Node opNode = node.getChildByFieldName("operator");
    ts::Node operandNode = node.getChildByFieldName("argument");

    if (opNode.isNull() || operandNode.isNull()) {
        throwError("Invalid unary operator: missing operator or argument", node);
    }

    std::string opText = getNodeText(opNode, source);
    UnaryOpType op = parseUnaryOpType(opText);
    auto operand = parseExpression(operandNode, source);

    return std::make_unique<UnaryOp>(op, std::move(operand));
}

ExprPtr PythonParser::parseIfExp(const ts::Node& node, std::string_view source) {
    // conditional_expression has no named fields, structure is:
    // child[0]: body (consequence)
    // child[1]: "if" keyword
    // child[2]: test (condition)
    // child[3]: "else" keyword
    // child[4]: orelse (alternative)

    if (node.getNumChildren() < 5) {
        throwError("Invalid conditional expression: missing body, test, or else clause", node);
    }

    ts::Node bodyNode = node.getChild(0);
    ts::Node testNode = node.getChild(2);
    ts::Node orelseNode = node.getChild(4);

    auto test = parseExpression(testNode, source);
    auto body = parseExpression(bodyNode, source);
    auto orelse = parseExpression(orelseNode, source);

    return std::make_unique<IfExp>(std::move(test), std::move(body), std::move(orelse));
}

ExprPtr PythonParser::parseSubscript(const ts::Node& node, std::string_view source) {
    ts::Node valueNode = node.getChildByFieldName("value");
    ts::Node subscriptNode = node.getChildByFieldName("subscript");

    if (valueNode.isNull() || subscriptNode.isNull()) {
        throwError("Invalid subscript: missing value or index", node);
    }

    auto value = parseExpression(valueNode, source);
    auto slice = parseExpression(subscriptNode, source);

    return std::make_unique<Subscript>(std::move(value), std::move(slice));
}

ExprPtr PythonParser::parseTuple(const ts::Node& node, std::string_view source) {
    auto tuple = std::make_unique<Tuple>();

    uint32_t childCount = node.getNumChildren();
    for (uint32_t i = 0; i < childCount; ++i) {
        ts::Node child = node.getChild(i);
        std::string childType{child.getType()};

        if (childType == "(" || childType == ")" || childType == "," || childType == "comment") {
            continue;  // Skip delimiters and comments
        }

        tuple->elts.push_back(parseExpression(child, source));
    }

    return tuple;
}

ExprPtr PythonParser::parseList(const ts::Node& node, std::string_view source) {
    auto list = std::make_unique<List>();

    uint32_t childCount = node.getNumChildren();
    for (uint32_t i = 0; i < childCount; ++i) {
        ts::Node child = node.getChild(i);
        std::string childType{child.getType()};

        if (childType == "[" || childType == "]" || childType == "," || childType == "comment") {
            continue;  // Skip delimiters and comments
        }

        list->elts.push_back(parseExpression(child, source));
    }

    return list;
}

ExprPtr PythonParser::parseDict(const ts::Node& node, std::string_view source) {
    auto dict = std::make_unique<Dict>();

    uint32_t childCount = node.getNumChildren();
    for (uint32_t i = 0; i < childCount; ++i) {
        ts::Node child = node.getChild(i);
        std::string childType{child.getType()};

        if (childType == "{" || childType == "}" || childType == "," || childType == "comment") {
            continue;  // Skip delimiters and comments
        }

        // Each child should be a "pair" node with key and value
        if (childType == "pair") {
            if (child.getNumChildren() >= 3) {
                // pair structure: key : value
                ts::Node keyNode = child.getChild(0);
                ts::Node valueNode = child.getChild(2);  // Skip the ':' at index 1

                dict->keys.push_back(parseExpression(keyNode, source));
                dict->values.push_back(parseExpression(valueNode, source));
            }
        }
    }

    return dict;
}

} // namespace epoch_script

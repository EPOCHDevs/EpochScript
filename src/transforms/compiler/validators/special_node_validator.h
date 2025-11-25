//
// Created by Claude Code
// EpochScript Special Node Validator Interface
//
// Provides pluggable validation system for transforms with special requirements.
// Validators can be registered per-transform and are automatically invoked during compilation.
//

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "../compilation_context.h"
#include "../parser/ast_nodes.h"

namespace epoch_script
{
    class TypeChecker;  // Forward declaration

    // Validation context passed to validators
    struct ValidationContext {
        const std::vector<strategy::InputValue>& args;
        const std::unordered_map<std::string, strategy::InputValue>& kwargs;
        const std::string& target_node_id;
        const std::string& component_name;
        TypeChecker& type_checker;
        CompilationContext& context;
    };

    // Interface for special node validators
    class ISpecialNodeValidator {
    public:
        virtual ~ISpecialNodeValidator() = default;

        // Validate inputs - throw std::runtime_error if invalid
        virtual void ValidateInputs(const ValidationContext& ctx) const = 0;

        // Get validator name for debugging
        virtual std::string GetName() const = 0;
    };

    // Registry for special node validators
    class SpecialNodeValidatorRegistry {
    public:
        static SpecialNodeValidatorRegistry& Instance();

        // Register a validator for a transform
        void Register(const std::string& transform_name,
                     std::shared_ptr<ISpecialNodeValidator> validator);

        // Check if transform has a special validator
        bool HasValidator(const std::string& transform_name) const;

        // Get validator for transform (nullptr if none)
        std::shared_ptr<ISpecialNodeValidator> GetValidator(
            const std::string& transform_name) const;

        // Validate if validator exists, otherwise no-op
        void ValidateIfNeeded(const ValidationContext& ctx) const;

    private:
        SpecialNodeValidatorRegistry() = default;
        std::unordered_map<std::string, std::shared_ptr<ISpecialNodeValidator>> validators_;
    };

    // Helper macro for validator registration
    #define REGISTER_SPECIAL_VALIDATOR(name, validator_class) \
        namespace { \
            struct validator_class##_registrar { \
                validator_class##_registrar() { \
                    SpecialNodeValidatorRegistry::Instance().Register( \
                        name, std::make_shared<validator_class>()); \
                } \
            }; \
            static validator_class##_registrar validator_class##_reg_instance; \
        }

} // namespace epoch_script

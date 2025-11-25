#pragma once
#include <string>
#include <unordered_map>
#include <fstream>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <vector>
#include <spdlog/spdlog.h>

namespace epoch_script {

class EnvLoader {
public:
    static EnvLoader& instance() {
        static EnvLoader instance;
        return instance;
    }

    // Get environment variable (checks loaded vars first, then system)
    std::string get(const std::string& key, const std::string& defaultValue = "") const {
        // Check loaded variables first
        auto it = m_variables.find(key);
        if (it != m_variables.end()) {
            return it->second;
        }

        // Fall back to system environment
        const char* value = std::getenv(key.c_str());
        return value ? value : defaultValue;
    }

    // Get as integer
    int getInt(const std::string& key, int defaultValue = 0) const {
        std::string value = get(key);
        if (value.empty()) return defaultValue;
        try {
            return std::stoi(value);
        } catch (const std::exception& exp) {
            SPDLOG_WARN("Failed to parse environment variable '{}' with value '{}' as integer: {}. Using default value: {}",
                        key, value, exp.what(), defaultValue);
            return defaultValue;
        } catch (...) {
            SPDLOG_WARN("Failed to parse environment variable '{}' with value '{}' as integer (unknown error). Using default value: {}",
                        key, value, defaultValue);
            return defaultValue;
        }
    }

    // Get as boolean
    bool getBool(const std::string& key, bool defaultValue = false) const {
        std::string value = get(key);
        if (value.empty()) return defaultValue;
        return value == "true" || value == "1" || value == "yes";
    }

    // Set variable (also sets in system environment)
    void set(const std::string& key, const std::string& value) {
        m_variables[key] = value;
        setenv(key.c_str(), value.c_str(), 1);
    }

private:
    std::unordered_map<std::string, std::string> m_variables;

    EnvLoader() {
        load();
    }

    // Load .env file (searches in order: .env.local, .env.<environment>, .env)
    void load(const std::string& envFile = "") {
        std::vector<std::string> filesToTry;

        // Determine which files to try
        if (!envFile.empty()) {
            filesToTry.push_back(envFile);
        } else {


            // Try in order of precedence
            filesToTry.emplace_back(".env.local");           // Local overrides (never commit)
        }

        // Load each file that exists
        for (const auto& file : filesToTry) {
            if (std::filesystem::exists(file)) {
                loadFile(file);
                SPDLOG_INFO("Loaded environment from {}.",  file);
            }
        }
    }

    void loadFile(const std::string& filename) {
        std::ifstream file(filename);
        if (!file.is_open()) return;

        std::string line;
        while (std::getline(file, line)) {
            parseLine(line);
        }
    }

    void parseLine(const std::string& line) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') return;

        // Find the = separator
        size_t pos = line.find('=');
        if (pos == std::string::npos) return;

        // Extract key and value
        std::string key = trim(line.substr(0, pos));
        std::string value = trim(line.substr(pos + 1));

        // Remove quotes if present
        if (value.length() >= 2) {
            if ((value.front() == '"' && value.back() == '"') ||
                (value.front() == '\'' && value.back() == '\'')) {
                value = value.substr(1, value.length() - 2);
            }
        }

        // Handle variable expansion ${VAR}
        value = expandVariables(value);

        // Store and set in environment
        if (!key.empty()) {
            m_variables[key] = value;
            setenv(key.c_str(), value.c_str(), 1);
        }
    }

    std::string expandVariables(const std::string& value) {
        std::string result = value;
        size_t pos = 0;

        while ((pos = result.find("${", pos)) != std::string::npos) {
            size_t end = result.find('}', pos);
            if (end == std::string::npos) break;

            std::string varName = result.substr(pos + 2, end - pos - 2);
            std::string varValue = get(varName);

            result.replace(pos, end - pos + 1, varValue);
            pos += varValue.length();
        }

        return result;
    }

    std::string trim(const std::string& str) {
        const auto strBegin = str.find_first_not_of(" \t\r\n");
        if (strBegin == std::string::npos) return "";

        const auto strEnd = str.find_last_not_of(" \t\r\n");
        return str.substr(strBegin, strEnd - strBegin + 1);
    }
};

// Convenience macro
#define ENV(key) epoch_script::EnvLoader::instance().get(key)
#define ENV_INT(key) epoch_script::EnvLoader::instance().getInt(key)
#define ENV_BOOL(key) epoch_script::EnvLoader::instance().getBool(key)

} // namespace epoch_script
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "helpers/helpers.h"

namespace helpers {



    class BaseOption {
    public:
        std::string const name;
        bool const required;

        /** Returns true if the option was supplied by the user.
         */
        bool isSpecified() const {
            return specified_;
        }
        
        virtual void print(std::ostream & s) {
            s << name << (required ? "*" : "") << (specified_ ? "[specified]" : "") << " = ";
        }

    protected:

        BaseOption(std::string const & name, bool required):
            name(name),
            required(required) {
        }

        BaseOption(std::string const & name, std::initializer_list<std::string> aliases, bool required):
            name(name),
            required(required),
            nameAliases_(aliases) {
        }


        virtual void parseValue(std::string const & str) = 0;

    private:
        friend class Settings;

        bool specified_;
        
        std::vector<std::string> nameAliases_;
    };

    template<typename T>
    class Option : public BaseOption {
    public:
        Option(std::string const & name, T const & defaultValue, bool required = true):
            BaseOption(name, required),
            value_(defaultValue) {
        }

        Option(std::string const & name, T const & defaultValue, std::initializer_list<std::string> aliases, bool required = true):
            BaseOption(name, aliases, required),
            value_(defaultValue) {
        }

        T const & value() const {
            return value_;
        }

        void print(std::ostream & s) override {
            BaseOption::print(s);
            s << value_;
        }

    protected:

        void parseValue(std::string const & s) override;

    private:

        T value_;
        
    }; // helpers::Option<T>

    template<>
    inline void Option<std::string>::parseValue(std::string const & s) {
        value_ = s;
    }

    template<>
    inline void Option<bool>::parseValue(std::string const & s) {
        if (s == "" || s == "1" || s == "t" || s == "true" || s == "T")
            value_ = true;
        else if (s == "0" || s == "f" || s == "false" || s == "F")
            value_ = false;
        else
            throw std::runtime_error(STR("Invalid value for argument " << name << ": expected boolean, but " << s << " found."));
    }

    template<>
    inline void Option<int>::parseValue(std::string const & s) {
        value_ = std::stoi(s);
    }

    template<>
    inline void Option<unsigned>::parseValue(std::string const & s) {
        value_ = std::stoul(s);
    }

    template<>
    inline void Option<uint64_t>::parseValue(std::string const & s) {
        value_ = std::stoull(s);
    }



    /** Manages settings for the application.
     */
    class Settings {
    public:
        /** Reads the settings from specified commandline.
         */
        void parse(int argc, char * argv[]) {
            for (int i = 0; i < argc; ++i) {
                parseSettingsLine(argv[i]);
            }
        }

        /** Checks that all options are properly initialized.
         */
        void check(bool allowUnknownOptions = false) {
            for (auto i : options_) {
                BaseOption & o = *i.second;
                if (o.required && ! o.isSpecified())
                    throw std::runtime_error(STR("Option " << o.name << " is required, but missing"));
            }
            if (! allowUnknownOptions && ! unknownOptions_.empty())
                throw std::runtime_error("Unrecognized options supplied");
        }

        /** Reads the settings from specified file.
         */
        void parse(std::string const & filename) {
            
        }

        void addOption(BaseOption & o) {
            addOption(o.name, &o);
            for (auto i : o.nameAliases_)
                addOption(i, &o);
        }



    private:

        friend std::ostream & operator << (std::ostream & s, Settings const & settings) {
            s << std::endl << "Options:" << std::endl;
            std::unordered_set<std::string> printed;
            for (auto i : settings.options_) {
                if (printed.find(i.second->name) == printed.end()) {
                    i.second->print(s);
                    s << std::endl;
                    printed.insert(i.second->name);
                }
            }
            s << std::endl << "Unknown options: " << std::endl;
            for (auto i : settings.unknownOptions_)
                s << i.first << " = " << i.second << std::endl;
            return s;
        }

        void addOption(std::string name, BaseOption * o) {
            auto i = options_.find(name);
            if (i != options_.end())
                throw std::runtime_error(STR("Option name " << name << " already used for option " + i->second->name));
            options_[name] = o;
        }

        void parseSettingsLine(char const * l) {
            size_t x = 0;
            std::string name;
            std::string value;
            while (l[x] != '=') {
                if (l[x] == 0) {
                    name = l;
                    value = "";
                    break;;
                }
                ++x;
            }
            if (l[x] != 0) {
                name = std::string(l, x);
                value = l + x + 1;
            }
            // find the specified option
            auto i = options_.find(name);
            if (i == options_.end()) {
                unknownOptions_[name] = value;
            } else {
                // TODO do we care about overwriting values? 
                i->second->parseValue(value);
                i->second->specified_ = true;
            }
        }
        
        std::unordered_map<std::string, BaseOption *> options_;
        
        std::unordered_map<std::string, std::string> unknownOptions_;

    }; // helpers::Settings



    
} // namespace helpers

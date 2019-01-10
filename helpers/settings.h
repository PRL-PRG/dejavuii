#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "helpers/helpers.h"

namespace helpers {



    class Option {
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

        Option(std::string const & name, bool required):
            name(name),
            required(required) {
        }

        Option(std::string const & name, std::initializer_list<std::string> aliases, bool required):
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

    class StringOption : public Option {
    public:
        StringOption(std::string const & name, std::string const & defaultValue, bool required = true):
            Option(name, required),
            value_(defaultValue) {
        }

        StringOption(std::string const & name, std::string const & defaultValue, std::initializer_list<std::string> aliases, bool required = true):
            Option(name, aliases, required),
            value_(defaultValue) {
        }

        std::string const & value() const {
            return value_;    
        }

        void print(std::ostream & s) override {
            Option::print(s);
            s << value_ << " [string]";
        }

    protected:

        void parseValue(std::string const & str) override {
            value_ = str;
        }
        
    private:

        friend std::ostream & operator << (std::ostream & s, StringOption const & o) {
            s << o.value_;
            return s;
        }
        
        std::string value_;

        
    }; // StringOption

    class BoolOption : public Option {
    public:

        BoolOption(std::string const & name, bool defaultValue, bool required = true):
            Option(name, required),
            value_(defaultValue) {
        }
        
        bool value() const {
            return value_;
        }

        void print(std::ostream & s) override {
            Option::print(s);
            s << value_ << " [bool]";
        }

    protected:

        void parseValue(std::string const & str) override {
            if (str == "" || str == "1" || str == "t" || str == "true" || str == "T")
                value_ = true;
            else if (str == "0" || str == "f" || str == "false" || str == "F")
                value_ = false;
            else
                throw std::runtime_error(STR("Invalid value for argument " << name << ": expected boolean, but " << str << " found."));
        }

    private:

        friend std::ostream & operator << (std::ostream & s, BoolOption const & o) {
            s << o.value_;
            return s;
        }

        bool value_;
        
    }; 


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
                Option & o = *i.second;
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

        void addOption(Option & o) {
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

        void addOption(std::string name, Option * o) {
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
        
        std::unordered_map<std::string, Option *> options_;
        
        std::unordered_map<std::string, std::string> unknownOptions_;

    }; // helpers::Settings



    
} // namespace helpers

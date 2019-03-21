#pragma once
#include <cassert>
#include <string>
#include <map>
#include <iostream>
#include <functional>

#include "helpers.h"

namespace helpers {

    typedef std::function<void(int, char * [])> CommandHandler;

    class Command {
    public:
        std::string const & name() const {
            return name_;
        }
        std::string const & description() const {
            return description_;
        }

        void operator () (int argc, char * argv[]) {
            handler_(argc, argv);
        }

        Command(std::string const & name, CommandHandler handler, std::string const & description):
            name_(name),
            handler_(handler),
            description_(description) {
            std::map<std::string, Command *> & commands = CommandsList();
            assert(commands.find(name) == commands.end() && "Command already exists");
            commands.insert(std::make_pair(name, this));
        }

        ~Command() {
            CommandsList().erase(this->name_);
        }

        static void PrintHelp(int argc, char * argv[]) {
            std::cerr << std::endl << "The following commands are understood: " << std::endl << std::endl;
            for (auto i : CommandsList()) {
                Command * c = i.second;
                std::cerr << c->name() << ": " << c->description() << std::endl; 
            }
            std::cerr << std::endl;
        }

        static void Execute(int argc, char * argv[]) {
            if (argc < 2) {
                PrintHelp(argc, argv);
                throw std::runtime_error("Command name missing. ");
            }
            std::string commandName = argv[1];
            std::map<std::string, Command *> & commands = CommandsList();
            auto i = commands.find(commandName);
            if (i == commands.end()) {
                PrintHelp(argc, argv);
                throw std::runtime_error(STR("Invalid command name: " << commandName));
            }
            (*(i->second))(argc - 2, argv + 2);
        }

    private:

        static std::map<std::string, Command *> & CommandsList() {
            static std::map<std::string, Command *> commands_;
            return commands_;
        }

        std::string name_;
        CommandHandler handler_;
        std::string description_;
        
    }; 


    
} // namespace helpers

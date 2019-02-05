#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "helpers/helpers.h"

namespace helpers {

    class SectionedTextFileReader {
    public:

        SectionedTextFileReader(char section_start = '#', char word_separator = ' '):
            section_start_(section_start),
            separator_(word_separator) {
        }

    protected:

        //virtual void section(std::vector<std::string> & r)  = 0;
        virtual void section_header(std::vector<std::string> & row)  = 0;
        virtual void row(std::vector<std::string> & row)  = 0;


//        virtual void error(std::ios_base::failure const & e) {
//            std::cout << "line " << lineNum_  << ": " << e.what() << std::endl;
//        }

        size_t parse(std::string const & filename) {

            unsigned int n_lines = 0;
            std::ifstream f = std::ifstream(filename, std::ios::in);

            while(!f.eof()) {
                std::string line;
                if (std::getline(f, line)) {
                    n_lines++;

                    std::vector<std::string> words;
                    boost::split(words, line, [this](char c){return c == this->separator_;});

                    if (words[0][0] == section_start_) {
                        // If the line starts with #, it's a section header.
                        section_header(words);
                    } else {
                        // Otherwise, it's an ordinary line.
                        row(words);
                    }
                }
            }

            f.close();
            return n_lines;
        }

    private:

        char section_start_;
        char separator_;
    }; // CSVReader


} // namespace stuffz

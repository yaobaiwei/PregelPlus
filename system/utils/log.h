#ifndef LOG_H
#define LOG_H
#include <iostream>
void logger(const char* str)
{
    if (_my_rank == 0)
        std::cout << str << std::endl;
}
#endif

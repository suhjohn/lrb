//
// Created by John Suh on 5/16/20.
//

#ifndef LRB_FILTER_H
#define LRB_FILTER_H

#include <map>
#include <string>
#include <memory>
#include "request.h"

class Filter;

class IFilterFactory {
public:
    IFilterFactory() {}

    virtual std::unique_ptr <Filter> create_unique() = 0;
};

class Filter {
public:
    Filter() {}

    virtual ~Filter() = default;

    virtual void init_with_params(const std::map <std::string, std::string> &params) {}

    // main filter management functions (to be defined by a policy)
    virtual bool should_filter(SimpleRequest &req) = 0;

    // returns the total bytes used for the filter
    virtual size_t total_bytes_used() = 0;

    // helper functions (factory pattern)
    static void registerType(std::string name, IFilterFactory *factory) {
        get_factory_instance()[name] = factory;
    }

    static std::unique_ptr <Filter> create_unique(std::string name) {
        std::unique_ptr <Filter> Filter_instance;
        auto m = get_factory_instance();
//        std::cerr << "create_unique" << endl;
//        for (map<std::string, IFilterFactory *>::iterator it = m.begin(); it != m.end(); ++it) {
//            std::cerr << it->first << "\n";
//        }
        if (get_factory_instance().count(name) != 1) {
            return nullptr;
        }
        Filter_instance = move(get_factory_instance()[name]->create_unique());
        return Filter_instance;
    }

    static std::map<std::string, IFilterFactory *> &get_factory_instance() {
        static std::map<std::string, IFilterFactory *> map_instance;
        return map_instance;
    }
};


template<class T>
class FilterFactory : public IFilterFactory {
public:
    FilterFactory(std::string name) {
        Filter::registerType(name, this);
    }

    std::unique_ptr <Filter> create_unique() {
        std::unique_ptr <Filter> newT(new T);
        return newT;
    }
};


#endif //LRB_FILTER_H

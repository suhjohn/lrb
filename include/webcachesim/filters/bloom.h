//
// Created by John Suh on 5/16/20.
//

#ifndef LRB_BLOOM_H
#define LRB_BLOOM_H


#include <sstream>
#include <vector>
#include <unordered_set>
#include <bf/bloom_filter/basic.hpp>
#include "filter.h"

using namespace std;

//class SetFilter : public Filter {
//public:
//    SetFilter() : Filter() {}
//
//    bool should_filter(SimpleRequest &req) {
//        return !exist_or_insert(req.get_id());
//    }
//
//    inline bool exist(uint64_t key) {
//        return (_filters[0].find(key)) != _filters[0].end() || (_filters[1].find(key) != _filters[1].end());
//    }
//
//    inline bool exist_or_insert(uint64_t key) {
//        if (exist(key))
//            return true;
//        else
//            insert(key);
//        return false;
//    }
//
//    inline void insert(uint64_t key) {
//        if (n_added_obj > max_n_element) {
//            _filters[1 - current_filter].clear();
//            current_filter = 1 - current_filter;
//            n_added_obj = 0;
//        }
//        _filters[current_filter].insert(key);
//        ++n_added_obj;
//    }
//
//    void init_with_params(const std::map <std::string, std::string> &params) override {
//        for (auto &it: params) {
//            if (it.first == "max_n_element") {
//                std::istringstream iss(it.second);
//                size_t new_n;
//                iss >> new_n;
//                max_n_element = new_n;
//            } else {
//                cerr << "Set filter unrecognized parameter: " << it.first << endl;
//            }
//        }
//        cerr << "Init Set filter. max_n_element: " << max_n_element;
//        unordered_set <uint64_t> filter_1;
//        unordered_set <uint64_t> filter_2;
//        _filters[0] = filter_1;
//        _filters[1] = filter_2;
//    }
//
//    size_t max_n_element = 40000000;
//    uint8_t current_filter = 0;
//    int n_added_obj = 0;
//    unordered_set <uint64_t> _filters[2];
//};
//
//static FilterFactory <SetFilter> factorySetFilter("Set");

class BloomFilter : public Filter {
public:
    BloomFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "bloom_k") {
                k = stoi(it.second);
            } else if (it.first == "fp_rate") {
                fp_rate = stod(it.second);
            } else {
                cerr << "Bloom filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Bloom filter. max_n_element: " << max_n_element << " fp_rate: " << fp_rate << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            bf::basic_bloom_filter b(fp_rate, max_n_element);
            filters.push_back(b);
        }
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;
    std::vector <bf::basic_bloom_filter> filters;
};

static FilterFactory <BloomFilter> factoryBloomFilter("Bloom");

#endif //LRB_BLOOM_H

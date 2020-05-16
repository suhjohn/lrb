//
// Created by John Suh on 5/16/20.
//

#include "bloom.h"

bool BloomFilter::should_filter(SimpleRequest &req) {
    auto key = req.get_id();
    for (int i = 0; i < k; i++) {
        if (filters[i]->lookup(key)) {
            return false;
        }
    }
    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx]->clear();
        n_added_obj = 0;
    }
    filters[curr_filter_idx]->add(key);
    // keep track of number of requests for the current filter
    ++n_added_obj;
    return true;
}

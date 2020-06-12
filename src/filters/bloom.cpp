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


bool SetFilter::should_filter(SimpleRequest &req) {
    auto key = req.get_id();
    for (int i = 0; i < k; i++) {
        if (filters[i].find(key) != filters[i].end()) {
            return false;
        }
    }
    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx].clear();
        n_added_obj = 0;
    }
    filters[curr_filter_idx].insert(key);
    // keep track of number of requests for the current filter
    ++n_added_obj;
    return true;
}


bool CountingSetFilter::should_filter(SimpleRequest &req) {
    auto key = req.get_id();
    for (int i = 0; i < k; i++) {
        if (filters[i].count(key)) {
            filters[i][key] += 1;
            return false;
        }
    }
    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx].clear();
        n_added_obj = 0;
    }
    filters[curr_filter_idx][key] += 1;
    // keep track of number of requests for the current filter
    ++n_added_obj;
    return true;
}


bool IgnoringKHitBloomFilter::should_filter(SimpleRequest &req) {
    auto key = req.get_id();
    for (int i = 0; i < k; i++) {
        // we have most likely seen it before
        if (filters[i]->lookup(key)) {
            bool _should_filter;

            // if we've seen it once before(i.e. current hit is second hit)
            if (count_maps[i][key] == 1) {
                _should_filter = false;
            } else { //
                // == 0(if we've not seen it before = false positive) or > 1(if current hit is at least the third hit)
                _should_filter = true;
            }
            count_maps[i][key] += 1;
            return _should_filter;
        }
    }
    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx]->clear();
        count_maps[curr_filter_idx].clear();
        n_added_obj = 0;
    }

    filters[curr_filter_idx]->add(key);
    count_maps[curr_filter_idx][key] += 1;
    // keep track of number of requests for the current filter
    ++n_added_obj;
    return true;
}

bool ResettingBloomFilter::should_filter(SimpleRequest &req) {
    auto key = req.get_id();
    for (int i = 0; i < k; i++) {
        if (filters[i]->lookup(key)) {
            bool should_filter;
            if (count_maps[i][key] % 2 == 1) {
                should_filter = false;
            } else {
                should_filter = true;
            }
            count_maps[i][key] += 1;
            return should_filter;
        }
    }

    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx]->clear();
        count_maps[curr_filter_idx].clear();
        n_added_obj = 0;
    }

    filters[curr_filter_idx]->add(key);
    count_maps[curr_filter_idx][key] += 1;
    ++n_added_obj;
    return true;
}

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
        filters[curr_filter_idx]->clear();
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
    bool is_new = true;
    for (int i = 0; i < k; i++) {
        if (filters[i]->lookup(key)) {
            // if we've seen it once before(i.e. current hit is second hit)
            if (seen_key_sets[i].find(key) != seen_key_sets[i].end()) {
                // Removing seen so that next time request with same key comes but key still exists in filter,
                //      it is ignored.
                seen_key_sets[i].erase(seen_key_sets[i].find(key));
                return false;
            } else { // if seen more than once before(which is why it won't exist in the seen_keys)
                is_new = false;
                break;
            }
        }
    }

    if (n_added_obj > max_n_element) {
        // clear the least recently used filter, which will now be our current filter
        curr_filter_idx = (curr_filter_idx + 1) % k;
        filters[curr_filter_idx]->clear();
        seen_key_sets[curr_filter_idx].clear();
        n_added_obj = 0;
    }

    filters[curr_filter_idx]->add(key);
    seen_key_sets[curr_filter_idx].insert(key);
    // keep track of number of requests for the current filter
    if (is_new) {
        ++n_added_obj;
    }
    return true;
}

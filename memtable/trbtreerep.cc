//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <iterator>
#include <memory>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <atomic>

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/mutexlock.h"
#include "util/threaded_rbtree.h"

namespace rocksdb {
namespace {

    class TRBTreeRep : public MemTableRep
    {
        typedef size_t size_type;

        // inner node use uintpre_t save pointer , save tags at lower bits
        typedef threaded_rbtree_node_t<uintptr_t, std::false_type> inner_node_t;
        // outer node use uint32 , save tags at higher bits
        typedef threaded_rbtree_node_t<uint32_t> outer_node_t;

        typedef threaded_rbtree_root_t<inner_node_t, std::false_type, std::true_type> inner_root_t;
        typedef threaded_rbtree_root_t<outer_node_t, std::true_type, std::true_type> outer_root_t;

        struct inner_holder_t
        {
            // inner root
            inner_root_t root;
            // version++ when split
            size_type version;
            // inner lock
            port::RWMutex mutex;
        };
        struct outer_element_t
        {
            // outer node
            outer_node_t node;
            // user key
            Slice bound;
        };

        struct deref_inner_key_t
        {
            char const *operator()(size_type p)
            {
                return (char const *)p + sizeof(inner_node_t);
            }
        };
        struct deref_outer_key_t
        {
            outer_element_t *array;
            Slice const &operator()(size_type p)
            {
                return array[p].bound;
            }
        };

        struct deref_inner_node_t
        {
            inner_node_t &operator()(size_type p)
            {
                return *(inner_node_t *)p;
            }
        };
        struct deref_outer_node_t
        {
            outer_element_t *array;
            outer_node_t &operator()(size_type p)
            {
                return array[p].node;
            }
        };

        struct inner_comparator_t
        {
            bool operator()(char const *l, char const *r) const
            {
                return c(l, r) < 0;
            }
            int compare(char const *l, char const *r) const
            {
                return c(l, r);
            }
            MemTableRep::KeyComparator const &c;
        };
        struct outer_comparator_t
        {
            bool operator()(Slice const &l, Slice const &r) const
            {
                return compare(l, r) < 0;
            }
            int compare(Slice const &l, Slice const &r) const
            {
                if (smallest != &l && smallest != &r)
                {
                    return c->Compare(l, r);
                }
                assert(&l != &r);
                return &r != smallest ? -1 : 1;
            }
            Slice const *smallest;
            Comparator const *c;
        };

        struct inner_comparator_ex_t
        {
            bool operator()(size_type l, size_type r) const
            {
                deref_inner_key_t deref_inner_key;
                int c = comp.compare(deref_inner_key(l), deref_inner_key(r));
                if (c == 0)
                {
                    return l < r;
                }
                return c < 0;
            }
            inner_comparator_t &comp;
        };
        struct outer_comparator_ex_t
        {
            bool operator()(size_type l, size_type r) const
            {
                if (l != 0 && r != 0)
                {
                    return comp.c->Compare(array[l].bound, array[r].bound) < 0;
                }
                return r != 0;
            }
            outer_comparator_t &comp;
            outer_element_t *array;
        };

        class key_set_t
        {
            inner_holder_t       **inner_holder_;       // inner trbtree holder
            outer_element_t       *outer_array_;        // outer node & key
            size_type              outer_capacity_;
            outer_root_t           outer_root_;
            port::RWMutex          outer_mutex_;
            Allocator             *allocator_;          // used for alloc inner_holder_t
            std::atomic_uintptr_t *memory_size_;
            inner_comparator_t     inner_comparator_;
            outer_comparator_t     outer_comparator_;

            const static size_type stack_max_depth = sizeof(uint32_t) * 16 - 3;
            const static size_type split_depth = 14;
            
            deref_inner_key_t deref_inner_key()
            {
                return deref_inner_key_t();
            }
            deref_inner_node_t deref_inner_node()
            {
                return deref_inner_node_t();
            }
            inner_comparator_ex_t inner_comparator_ex()
            {
                return inner_comparator_ex_t{inner_comparator_};
            }

            deref_outer_key_t deref_outer_key()
            {
                return deref_outer_key_t{outer_array_};
            }
            deref_outer_node_t deref_outer_node()
            {
                return deref_outer_node_t{outer_array_};
            }
            outer_comparator_ex_t outer_comparator_ex()
            {
                return outer_comparator_ex_t{outer_comparator_, outer_array_};
            }

        public:
            key_set_t(const MemTableRep::KeyComparator &c, Allocator *a, std::atomic_uintptr_t *m)
                : allocator_(a)
                , memory_size_(m)
                , inner_comparator_{c}
            {
                auto key_comparator = c.icomparator();
                outer_comparator_.c = key_comparator->user_comparator();

                inner_holder_   = (inner_holder_t **)malloc(sizeof(inner_holder_t *));
                outer_array_    = (outer_element_t *)malloc(sizeof(outer_element_t));
                outer_capacity_ = 1;
                *memory_size_   = sizeof(outer_element_t) + sizeof(inner_root_t);
                *inner_holder_  = (inner_holder_t *)allocator_->AllocateAligned(sizeof(inner_holder_t));
                new(*inner_holder_) inner_holder_t();
                (*inner_holder_)->version = 0;
                new(outer_array_) outer_element_t();
                outer_comparator_.smallest = &outer_array_->bound;

                threaded_rbtree_stack_t<outer_node_t, stack_max_depth> stack;
                threaded_rbtree_find_path_for_multi(outer_root_,
                                                    stack,
                                                    deref_outer_node(),
                                                    0,
                                                    outer_comparator_ex());
                threaded_rbtree_insert(outer_root_, stack, deref_outer_node(), 0);
                assert(outer_root_.get_count() == 1);
            }

            ~key_set_t()
            {
                for (size_type i = 0; i < outer_root_.get_count(); ++i)
                {
                    inner_holder_[i]->~inner_holder_t();
                    outer_array_[i].bound.~Slice();
                }
                free(inner_holder_);
                free(outer_array_);
            }

            template<class Lock>
            struct iterator
            {
                key_set_t      *key_set_;
                size_type       inner_index_;
                size_type       inner_node_;
                size_type       version_;

            public:
                iterator(key_set_t *key_set)
                {
                    key_set_ = key_set;
                    inner_index_ = outer_node_t::nil_sentinel;
                    inner_node_ = inner_node_t::nil_sentinel;
                    version_ = 0;
                }

                bool valid()
                {
                    return inner_node_ != inner_node_t::nil_sentinel;
                }
                const char *key()
                {
                    assert(inner_node_ != inner_node_t::nil_sentinel);
                    return key_set_->deref_inner_key()(inner_node_);
                }

                bool seek_to_first()
                {
                    Lock outer_lock(&key_set_->outer_mutex_);
                    inner_index_ = key_set_->outer_root_.get_most_left(key_set_->deref_outer_node());
                    auto inner_holder = key_set_->inner_holder_[inner_index_];

                    Lock inner_lock(&inner_holder->mutex);
                    inner_node_ = inner_holder->root.get_most_left(key_set_->deref_inner_node());
                    version_ = inner_holder->version;

                    return inner_node_ == inner_node_t::nil_sentinel;
                }

                bool seek_to_last()
                {
                    Lock outer_lock(&key_set_->outer_mutex_);
                    inner_index_ = key_set_->outer_root_.get_most_right(key_set_->deref_outer_node());
                    auto inner_holder = key_set_->inner_holder_[inner_index_];

                    Lock inner_lock(&inner_holder->mutex);
                    inner_node_ = inner_holder->root.get_most_right(key_set_->deref_inner_node());
                    version_ = inner_holder->version;

                    return inner_node_ == inner_node_t::nil_sentinel;
                }

                bool seek(const char *target)
                {
                    Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(target));
                    Lock outer_lock(&key_set_->outer_mutex_);
                    // search outer
                    inner_index_ = threaded_rbtree_reverse_lower_bound(key_set_->outer_root_,
                                                                       key_set_->deref_outer_node(),
                                                                       user_key,
                                                                       key_set_->deref_outer_key(),
                                                                       key_set_->outer_comparator_);
                    auto inner_holder = key_set_->inner_holder_[inner_index_];
                    bool move_next;
                    {
                        Lock inner_lock(&inner_holder->mutex);
                        // search inner
                        inner_node_ = threaded_rbtree_lower_bound(inner_holder->root,
                                                                  key_set_->deref_inner_node(),
                                                                  target,
                                                                  key_set_->deref_inner_key(),
                                                                  key_set_->inner_comparator_);
                        version_ = inner_holder->version;
                        move_next = inner_node_ == inner_node_t::nil_sentinel;
                    }
                    // at inner end , go next inner rbtree
                    if (move_next)
                    {
                        if (inner_index_ == key_set_->outer_root_.get_most_right(key_set_->deref_outer_node()))
                        {
                            // outer end ...
                            inner_node_ = inner_node_t::nil_sentinel;
                            return false;
                        }
                        inner_index_ = threaded_rbtree_move_next(inner_index_, key_set_->deref_outer_node());
                        inner_holder = key_set_->inner_holder_[inner_index_];

                        Lock inner_lock(&inner_holder->mutex);
                        inner_node_ = inner_holder->root.get_most_left(key_set_->deref_inner_node());
                        version_ = inner_holder->version;
                        assert(inner_node_ != inner_node_t::nil_sentinel);
                    }
                    return true;
                }
                bool seek_for_prev(const char *target)
                {
                    // symmetric with seek(const char *)
                    Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(target));
                    Lock outer_lock(&key_set_->outer_mutex_);
                    inner_index_ = threaded_rbtree_reverse_lower_bound(key_set_->outer_root_,
                                                                       key_set_->deref_outer_node(),
                                                                       user_key,
                                                                       key_set_->deref_outer_key(),
                                                                       key_set_->outer_comparator_);
                    auto inner_holder = key_set_->inner_holder_[inner_index_];
                    bool move_prev;
                    {
                        Lock inner_lock(&inner_holder->mutex);
                        inner_node_ = threaded_rbtree_reverse_lower_bound(inner_holder->root,
                                                                          key_set_->deref_inner_node(),
                                                                          target,
                                                                          key_set_->deref_inner_key(),
                                                                          key_set_->inner_comparator_);
                        version_ = inner_holder->version;
                        move_prev = inner_node_ == inner_node_t::nil_sentinel;
                    }
                    if (move_prev)
                    {
                        if (inner_index_ == key_set_->outer_root_.get_most_left(key_set_->deref_outer_node()))
                        {
                            inner_node_ = inner_node_t::nil_sentinel;
                            return false;
                        }
                        inner_index_ = threaded_rbtree_move_prev(inner_index_, key_set_->deref_outer_node());
                        inner_holder = key_set_->inner_holder_[inner_index_];

                        Lock inner_lock(&inner_holder->mutex);
                        inner_node_ = inner_holder->root.get_most_right(key_set_->deref_inner_node());
                        version_ = inner_holder->version;
                        assert(inner_node_ != inner_node_t::nil_sentinel);
                    }
                    return true;
                }

                bool next()
                {
                    Lock outer_lock(&key_set_->outer_mutex_);
                    auto inner_holder = key_set_->inner_holder_[inner_index_];
                    bool expired;
                    while (true)
                    {
                        Lock inner_lock(&inner_holder->mutex);
                        if (inner_holder->version != version_)
                        {
                            // wrong lock ...
                            expired = true;
                            break;
                        }
                        inner_node_ = threaded_rbtree_move_next(inner_node_, key_set_->deref_inner_node());
                        if (inner_node_ != inner_node_t::nil_sentinel)
                        {
                            return true;
                        }
                        expired = false;
                        break;
                    }
                    if (expired)
                    {
                        // expired , inner rbtree has splited , re-search
                        Slice user_key =
                            ExtractUserKey(GetLengthPrefixedSlice(key_set_->deref_inner_key()(inner_node_)));
                        inner_index_ = threaded_rbtree_reverse_lower_bound(key_set_->outer_root_,
                                                                           key_set_->deref_outer_node(),
                                                                           user_key,
                                                                           key_set_->deref_outer_key(),
                                                                           key_set_->outer_comparator_);
                        inner_holder = key_set_->inner_holder_[inner_index_];
                        version_ = inner_holder->version;

                        Lock inner_lock(&inner_holder->mutex);
                        inner_node_ = threaded_rbtree_move_next(inner_node_, key_set_->deref_inner_node());
                        if (inner_node_ != inner_node_t::nil_sentinel)
                        {
                            return true;
                        }
                    }
                    // move next
                    if (inner_index_ == key_set_->outer_root_.get_most_right(key_set_->deref_outer_node()))
                    {
                        inner_node_ = inner_node_t::nil_sentinel;
                        return false;
                    }
                    inner_index_ = threaded_rbtree_move_next(inner_index_, key_set_->deref_outer_node());
                    inner_holder = key_set_->inner_holder_[inner_index_];

                    Lock inner_lock(&inner_holder->mutex);
                    inner_node_ = inner_holder->root.get_most_left(key_set_->deref_inner_node());
                    version_ = inner_holder->version;
                    assert(inner_node_ != inner_node_t::nil_sentinel);
                    return true;
                }
                bool prev()
                {
                    // symmetric with next()
                    Lock outer_lock(&key_set_->outer_mutex_);
                    auto inner_holder = key_set_->inner_holder_[inner_index_];
                    bool expired;
                    while (true)
                    {
                        Lock inner_lock(&inner_holder->mutex);
                        if (inner_holder->version != version_)
                        {
                            expired = true;
                            break;
                        }
                        inner_node_ = threaded_rbtree_move_prev(inner_node_, key_set_->deref_inner_node());
                        if (inner_node_ != inner_node_t::nil_sentinel)
                        {
                            return true;
                        }
                        expired = false;
                        break;
                    }
                    if (expired)
                    {
                        Slice user_key =
                            ExtractUserKey(GetLengthPrefixedSlice(key_set_->deref_inner_key()(inner_node_)));
                        inner_index_ = threaded_rbtree_reverse_lower_bound(key_set_->outer_root_,
                                                                           key_set_->deref_outer_node(),
                                                                           user_key,
                                                                           key_set_->deref_outer_key(),
                                                                           key_set_->outer_comparator_);
                        inner_holder = key_set_->inner_holder_[inner_index_];
                        version_ = inner_holder->version;

                        Lock inner_lock(&inner_holder->mutex);
                        inner_node_ = threaded_rbtree_move_prev(inner_node_, key_set_->deref_inner_node());
                        if (inner_node_ != inner_node_t::nil_sentinel)
                        {
                            return true;
                        }
                    }
                    if (inner_index_ == key_set_->outer_root_.get_most_left(key_set_->deref_outer_node()))
                    {
                        inner_node_ = inner_node_t::nil_sentinel;
                        return false;
                    }
                    inner_index_ = threaded_rbtree_move_prev(inner_index_, key_set_->deref_outer_node());
                    inner_holder = key_set_->inner_holder_[inner_index_];

                    Lock inner_lock(&inner_holder->mutex);
                    inner_node_ = inner_holder->root.get_most_right(key_set_->deref_inner_node());
                    version_ = inner_holder->version;
                    assert(inner_node_ != inner_node_t::nil_sentinel);
                    return true;
                }
            };

            template<class Lock>
            bool contains(const char *key)
            {
                Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(key));
                size_type inner_index;
                size_type inner_node;
                {
                    Lock outer_lock(&outer_mutex_);
                    inner_index = threaded_rbtree_reverse_lower_bound(outer_root_,
                                                                      deref_outer_node(),
                                                                      user_key,
                                                                      deref_outer_key(),
                                                                      outer_comparator_);

                    Lock inner_lock(&inner_holder_[inner_index]->mutex);
                    inner_node = threaded_rbtree_equal_unique(inner_holder_[inner_index]->root,
                                                              deref_inner_node(),
                                                              key,
                                                              deref_inner_key(),
                                                              inner_comparator_);
                }
                return inner_node != inner_node_t::nil_sentinel;
            }

            template<class Lock>
            size_type approximate_range_count(const char *start, const char *end, size_type count)
            {
                Slice start_user_key = ExtractUserKey(GetLengthPrefixedSlice(start));
                Slice end_user_key = ExtractUserKey(GetLengthPrefixedSlice(end));
                Lock outer_lock(&outer_mutex_);
                double start_ratio = threaded_rbtree_approximate_rank_ratio(outer_root_,
                                                                            deref_outer_node(),
                                                                            start_user_key,
                                                                            deref_outer_key(),
                                                                            outer_comparator_);
                double end_ratio = threaded_rbtree_approximate_rank_ratio(outer_root_,
                                                                          deref_outer_node(),
                                                                          end_user_key,
                                                                          deref_outer_key(),
                                                                          outer_comparator_);
                assert(end_ratio >= start_ratio);
                if (end_ratio - start_ratio >= std::numeric_limits<double>::epsilon())
                {
                    return size_type(count * (end_ratio - start_ratio));
                }
                size_type inner_index = threaded_rbtree_reverse_lower_bound(outer_root_,
                                                                            deref_outer_node(),
                                                                            start_user_key,
                                                                            deref_outer_key(),
                                                                            outer_comparator_);
                Lock inner_lock(&inner_holder_[inner_index]->mutex);
                start_ratio = threaded_rbtree_approximate_rank_ratio(inner_holder_[inner_index]->root,
                                                                     deref_inner_node(),
                                                                     start,
                                                                     deref_inner_key(),
                                                                     inner_comparator_);
                end_ratio = threaded_rbtree_approximate_rank_ratio(inner_holder_[inner_index]->root,
                                                                   deref_inner_node(),
                                                                   end,
                                                                   deref_inner_key(),
                                                                   inner_comparator_);
                assert(end_ratio >= start_ratio);
                return size_type(count * ((end_ratio - start_ratio) / outer_root_.get_count()));
            }

            void insert(KeyHandle handle)
            {
                size_type inner_node = reinterpret_cast<size_type>(handle);
                Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(deref_inner_key()(inner_node)));
                size_type inner_index;
                size_type outer_count;
                size_type version;
                size_type height;
                inner_root_t *inner_root;
                {
                    ReadLock outer_lock(&outer_mutex_);
                    // search outer rbtree
                    inner_index = threaded_rbtree_reverse_lower_bound(outer_root_,
                                                                      deref_outer_node(),
                                                                      user_key,
                                                                      deref_outer_key(),
                                                                      outer_comparator_);
                    outer_count = outer_root_.get_count();

                    WriteLock inner_lock(&inner_holder_[inner_index]->mutex);
                    // insert inner rbtree
                    version = inner_holder_[inner_index]->version;
                    inner_root = &inner_holder_[inner_index]->root;
                    threaded_rbtree_stack_t<inner_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(*inner_root,
                                                        stack,
                                                        deref_inner_node(),
                                                        inner_node,
                                                        inner_comparator_ex());
                    height = stack.height;
                    threaded_rbtree_insert(*inner_root, stack, deref_inner_node(), inner_node);
                }
                if (height <= split_depth || outer_count + 1 == outer_node_t::nil_sentinel)
                {
                    return;
                }
                WriteLock outer_lock(&outer_mutex_);
                outer_count = outer_root_.get_count();
                // double check
                if (version != inner_holder_[inner_index]->version || outer_count + 1 == outer_node_t::nil_sentinel)
                {
                    return;
                }
                WriteLock inner_lock(&inner_holder_[inner_index]->mutex);
                size_type root = inner_root->root.root;
                Slice bound = ExtractUserKey(GetLengthPrefixedSlice(deref_inner_key()(root)));
                // don't use if (bound == outer_array_[inner_index].bound)
                // when inner_index == 0 , bound is the smallest , but it's empty
                if (outer_comparator_.compare(bound, outer_array_[inner_index].bound) == 0)
                {
                    // OMG ! there are so many same user keys in diff seqno
                    // can't split ...
                    return;
                }
                assert(outer_comparator_.compare(bound, outer_array_[inner_index].bound) > 0);
                // grow
                if (outer_count == outer_capacity_)
                {
                    outer_capacity_ *= 2;
                    inner_holder_ =
                        (inner_holder_t **)realloc(inner_holder_, sizeof(inner_holder_t *) * outer_capacity_);
                    outer_array_ =
                        (outer_element_t *)realloc(outer_array_, sizeof(outer_element_t) * outer_capacity_);
                    outer_comparator_.smallest = &outer_array_->bound;
                    *memory_size_ += (sizeof(outer_element_t) + sizeof(inner_root_t)) * outer_count;
                }
                // prepare new outer element & inner holder
                auto new_inner_holder = (inner_holder_t *)allocator_->AllocateAligned(sizeof(inner_holder_t));
                new(new_inner_holder) inner_holder_t();
                new(outer_array_ + outer_count) outer_element_t();
                inner_root_t *split_root = &new_inner_holder->root;
                ++inner_holder_[inner_index]->version;
                new_inner_holder->version = inner_holder_[inner_index]->version;

                // base check
                deref_inner_node_t deref = deref_inner_node();
                assert(deref(root).left_is_child());
                assert(deref(root).right_is_child());
                size_type prev = threaded_rbtree_move_prev(root, deref);
                size_type next = threaded_rbtree_move_next(root, deref);
                assert(deref(prev).right_is_thread());
                assert(deref(next).left_is_thread());

                // fix new rbtree
                split_root->root.root = deref(root).right_get_link();
                deref(split_root->root.root).set_black();
                split_root->root.set_right(inner_root->get_most_right(deref));
                split_root->root.set_left(next);
                deref(next).left_set_link(inner_node_t::nil_sentinel);

                // fix old rbtree
                inner_root->root.root = deref(root).left_get_link();
                deref(inner_root->root.root).set_black();
                inner_root->root.set_right(prev);
                deref(prev).right_set_link(inner_node_t::nil_sentinel);

                {
                    // insert root into new rbtree
                    threaded_rbtree_stack_t<inner_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(*split_root, stack, deref, root, inner_comparator_ex());
                    threaded_rbtree_insert(*split_root, stack, deref, root);
                    // move internal keys under bound to new rbtree
                    for (size_type most_right = inner_root->get_most_right(deref);
                         ExtractUserKey(GetLengthPrefixedSlice(deref_inner_key()(most_right))) == bound;
                         most_right = inner_root->get_most_right(deref)) {
                        threaded_rbtree_find_path_for_remove(*inner_root,
                                                             stack,
                                                             deref,
                                                             most_right,
                                                             inner_comparator_ex());
                        threaded_rbtree_remove(*inner_root, stack, deref);
                        threaded_rbtree_find_path_for_multi(*split_root, stack, deref, most_right, inner_comparator_ex());
                        threaded_rbtree_insert(*split_root, stack, deref, most_right);
                    }
                }
                {
                    // insert new tbtree into outer
                    inner_holder_[outer_count] = new_inner_holder;
                    outer_array_[outer_count].bound = bound;
                    threaded_rbtree_stack_t<outer_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(outer_root_,
                                                        stack,
                                                        deref_outer_node(),
                                                        outer_count,
                                                        outer_comparator_ex());
                    threaded_rbtree_insert(outer_root_, stack, deref_outer_node(), outer_count);
                }
                assert(outer_count + 1 == outer_root_.get_count());
            }
        };

        // used for immutable
        struct DummyLock
        {
            template<class T> DummyLock(T const &) {}
        };


    public:
        std::atomic_uintptr_t memory_size_;
        mutable key_set_t key_set_;
        std::atomic_bool immutable_;
        std::atomic_size_t num_entries;

    public:
        explicit TRBTreeRep(const MemTableRep::KeyComparator &compare, Allocator *allocator,
                            const SliceTransform *) : MemTableRep(allocator),
                                                      memory_size_(0),
                                                      key_set_(compare, allocator, &memory_size_),
                                                      immutable_(false),
                                                      num_entries(0)
        {
        }

        virtual KeyHandle Allocate(const size_t len, char **buf) override
        {
            char *mem = allocator_->AllocateAligned(sizeof(inner_node_t) + len);
            *buf = mem + sizeof(inner_node_t);
            return static_cast<KeyHandle>(mem);
        }

        // Insert key into the list.
        // REQUIRES: nothing that compares equal to key is currently in the list.
        virtual void Insert(KeyHandle handle) override
        {
            key_set_.insert(handle);
            ++num_entries;
        }

        // Like Insert(handle), but may be called concurrent with other calls
        // to InsertConcurrently for other handles
        virtual void InsertConcurrently(KeyHandle handle) {
            key_set_.insert(handle);
            ++num_entries;
        }

        // Returns true iff an entry that compares equal to key is in the list.
        virtual bool Contains(const Slice &internal_key) const override
        {
            std::string memtable_key;
            EncodeKey(&memtable_key, internal_key);
            if (immutable_)
            {
                return key_set_.contains<DummyLock>(memtable_key.data());
            }
            else
            {
                return key_set_.contains<ReadLock>(memtable_key.data());
            }
        }

        virtual void MarkReadOnly() override
        {
            immutable_ = true;
        }

        virtual size_t ApproximateMemoryUsage() override
        {
            return memory_size_.load();
        }

        virtual uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                               const Slice& end_ikey) override {
            std::string start_tmp, end_tmp;
            if (immutable_)
            {
                return key_set_.approximate_range_count<DummyLock>(EncodeKey(&start_tmp, start_ikey),
                                                                   EncodeKey(&end_tmp, end_ikey),
                                                                   num_entries);
            }
            else
            {
                return key_set_.approximate_range_count<ReadLock>(EncodeKey(&start_tmp, start_ikey),
                                                                  EncodeKey(&end_tmp, end_ikey),
                                                                  num_entries);
            }
        }

        virtual void
        Get(const LookupKey &k, void *callback_args,
            bool (*callback_func)(void *arg, const KVGetter*)) override
        {
            CompositeKVGetter getter;
            if (immutable_)
            {
                key_set_t::iterator<DummyLock> iter(&key_set_);
                char const *key;
                if (!iter.seek(k.memtable_key().data()))
                {
                    return;
                }
                key = iter.key();
                while (callback_func(callback_args, getter.SetKey(key)))
                {
                    if (!iter.next())
                    {
                        return;
                    }
                    key = iter.key();
                }
            }
            else
            {
                key_set_t::iterator<ReadLock> iter(&key_set_);
                char const *key;
                if (!iter.seek(k.memtable_key().data()))
                {
                    return;
                }
                key = iter.key();
                while (callback_func(callback_args, getter.SetKey(key)))
                {
                    if (!iter.next())
                    {
                        return;
                    }
                    key = iter.key();
                }
            }
        }

        virtual ~TRBTreeRep() override
        {
        }

        template<class Lock>
        class Iterator : public MemTableRep::Iterator
        {
            mutable key_set_t::iterator<Lock> iter_;
            mutable std::string tmp_key_;

            friend class TRBTreeRep;

            Iterator(key_set_t *tree) : iter_(tree)
            {
            }

        public:
            virtual ~Iterator() override{}

            // Returns true iff the iterator is positioned at a valid node.
            virtual bool Valid() const override
            {
                return iter_.valid();
            }

            // Returns the key at the current position.
            // REQUIRES: Valid()
            virtual const char *key() const override
            {
                return iter_.key();
            }

            // Advances to the next position.
            // REQUIRES: Valid()
            virtual void Next() override
            {
                iter_.next();
            }

            // Advances to the previous position.
            // REQUIRES: Valid()
            virtual void Prev() override
            {
                iter_.prev();
            }

            // Advance to the first entry with a key >= target
            virtual void Seek(const Slice &user_key, const char *memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    iter_.seek(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    iter_.seek(tmp_key_.c_str());
                }
            }

            // retreat to the first entry with a key <= target
            virtual void SeekForPrev(const Slice& user_key, const char* memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    iter_.seek_for_prev(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    iter_.seek_for_prev(tmp_key_.c_str());
                }
            }

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToFirst() override
            {
                iter_.seek_to_first();
            }

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToLast() override
            {
                iter_.seek_to_last();
            }

            virtual bool IsSeekForPrevSupported() const override
            {
                return true;
            }
        };
        virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override
        {
            if(immutable_)
            {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(TRBTreeRep::Iterator<DummyLock>))
                              : operator new(sizeof(TRBTreeRep::Iterator<DummyLock>));
                return new(mem) TRBTreeRep::Iterator<DummyLock>(&key_set_);
            }
            else
            {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(TRBTreeRep::Iterator<ReadLock>))
                              : operator new(sizeof(TRBTreeRep::Iterator<ReadLock>));
                return new(mem) TRBTreeRep::Iterator<ReadLock>(&key_set_);
            }
        }
    };

    class TRBTreeMemTableRepFactory : public MemTableRepFactory
    {
    public:
        virtual ~TRBTreeMemTableRepFactory(){}

        using MemTableRepFactory::CreateMemTableRep;
        virtual MemTableRep *CreateMemTableRep(
                const MemTableRep::KeyComparator &compare, Allocator *allocator,
                const SliceTransform *transform, Logger *logger) override
        {
          return new TRBTreeRep(compare, allocator, transform);
        }

        virtual const char *Name() const override
        {
          return "TRBTreeMemTableRepFactory";
        }

        virtual bool IsInsertConcurrentlySupported() const override
        {
            return true;
        }
    };
}

MemTableRepFactory *NewThreadedRBTreeRepFactory()
{
  return new TRBTreeMemTableRepFactory();
}

} // namespace rocksdb

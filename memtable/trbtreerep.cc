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
#include "threaded_rb_tree.h"

namespace rocksdb {
namespace {
    class FallbackDetail
    {
        typedef Slice bound_t;
        MemTableRep::KeyComparator const &c;
    };
    class BytewistDetail
    {
        typedef std::array<uint64_t, 3> bound_t;
    };
    
    class TRBTreeRep : public MemTableRep
    {
        typedef std::size_t size_type;

        struct inner_node_t
        {
            typedef uintptr_t index_type;

            index_type children[2];

            inner_node_t(index_type left, index_type right)
            {
                children[0] = left;
                children[1] = right;
            }
            inner_node_t()
            {
            }

            static size_type constexpr flag_bit_mask = index_type(1);
            static size_type constexpr type_bit_mask = index_type(2);
            static size_type constexpr full_bit_mask = flag_bit_mask | type_bit_mask;

            static size_type constexpr nil_sentinel = ~index_type(0) & ~full_bit_mask;

            bool left_is_child() const
            {
                return (children[0] & type_bit_mask) == 0;
            }
            bool right_is_child() const
            {
                return (children[1] & type_bit_mask) == 0;
            }
            bool left_is_thread() const
            {
                return (children[0] & type_bit_mask) != 0;
            }
            bool right_is_thread() const
            {
                return (children[1] & type_bit_mask) != 0;
            }
            void left_set_child()
            {
                children[0] &= ~type_bit_mask;
            }
            void right_set_child()
            {
                children[1] &= ~type_bit_mask;
            }
            void left_set_thread()
            {
                children[0] |= type_bit_mask;
            }
            void right_set_thread()
            {
                children[1] |= type_bit_mask;
            }
            void left_set_link(size_type link)
            {
                children[0] = index_type((children[0] & full_bit_mask) | link);
            }
            void right_set_link(size_type link)
            {
                children[1] = index_type((children[1] & full_bit_mask) | link);
            }
            size_type left_get_link() const
            {
                return children[0] & ~full_bit_mask;
            }
            size_type right_get_link() const
            {
                return children[1] & ~full_bit_mask;
            }
            bool is_used() const
            {
                return (children[1] & flag_bit_mask) == 0;
            }
            void set_used()
            {
                children[1] &= ~flag_bit_mask;
            }
            bool is_empty() const
            {
                return (children[1] & flag_bit_mask) != 0;
            }
            void set_empty()
            {
                children[1] |= flag_bit_mask;
            }
            bool is_black() const
            {
                return (children[0] & flag_bit_mask) == 0;
            }
            void set_black()
            {
                children[0] &= ~flag_bit_mask;
            }
            bool is_red() const
            {
                return (children[0] & flag_bit_mask) != 0;
            }
            void set_red()
            {
                children[0] |= flag_bit_mask;
            }
        };
        typedef threaded_rbtree_node_t<uint32_t> outer_node_t;
        typedef threaded_rbtree_root_t<inner_node_t, std::false_type, std::true_type> inner_root_t;
        typedef threaded_rbtree_root_t<outer_node_t, std::false_type, std::true_type> outer_root_t;

        struct inner_holder_t
        {
            inner_root_t root;
            size_t version;
            port::RWMutex mutex;
        };
        struct outer_element_t
        {
            outer_node_t node;
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
            Slice operator()(size_type p)
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
                if (max_element != &l && max_element != &r)
                {
                    return c->Compare(l, r) < 0;
                }
                assert(l != r);
                return &l != max_element;
            }
            int compare(Slice const &l, Slice const &r) const
            {
                if (max_element != &l && max_element != &r)
                {
                    return c->Compare(l, r);
                }
                assert(l != r);
                return &l == max_element ? 1 : -1;
            }
            Slice const *max_element;
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
                int c = comp.compare(array[l].bound, array[r].bound);
                if (c == 0)
                {
                    return l < r;
                }
                return c < 0;
            }
            outer_comparator_t &comp;
            outer_element_t *array;
        };

        class key_set_t
        {
            inner_holder_t       **inner_holder_;
            outer_element_t       *outer_array_;
            size_type              outer_count_;
            size_type              outer_count_max_;
            outer_root_t           outer_root_;
            port::RWMutex          outer_mutex_;
            Allocator             *allocator_;
            std::atomic_uintptr_t *memory_size_;
            inner_comparator_t     inner_comparator_;
            outer_comparator_t     outer_comparator_;

            const static size_type stack_max_depth = sizeof(uint32_t) * 16 - 3;
            const static size_type split_max_depth = 14;
            
            deref_inner_key_t deref_inner_key()
            {
                return deref_inner_key_t();
            }
            deref_inner_node_t deref_inner_node()
            {
                return deref_inner_node_t();
            }

            deref_outer_key_t deref_outer_key()
            {
                return deref_outer_key_t{outer_array_};
            }
            deref_outer_node_t deref_outer_node()
            {
                return deref_outer_node_t{outer_array_};
            }

        public:
            key_set_t(const MemTableRep::KeyComparator &c, Allocator *a, std::atomic_uintptr_t *m)
                : allocator_(a)
                , memory_size_(m)
                , inner_comparator_{c}
            {
                assert(dynamic_cast<const MemTable::KeyComparator *>(&c) != nullptr);
                auto key_comparator = static_cast<const MemTable::KeyComparator *>(&c);
                outer_comparator_.c = key_comparator->comparator.user_comparator();

                inner_holder_ = (inner_holder_t **)malloc(sizeof(inner_holder_t *));
                *inner_holder_ = (inner_holder_t *)allocator_->AllocateAligned(sizeof(inner_holder_t));
                outer_array_ = (outer_element_t *)malloc(sizeof(outer_element_t));
                outer_count_ = 1;
                outer_count_max_ = 1;
                outer_comparator_.max_element = &outer_array_->bound;
                *memory_size_ = sizeof(outer_element_t) + sizeof(inner_root_t);
                new(*inner_holder_) inner_holder_t();
                (*inner_holder_)->version = 0;

                threaded_rbtree_stack_t<outer_node_t, stack_max_depth> stack;
                threaded_rbtree_find_path_for_multi(outer_root_, stack, deref_outer_node(), 0, outer_comparator_t());
                threaded_rbtree_insert(outer_root_, stack, deref_outer_node(), 0);
            }

            ~key_set_t()
            {
                for (size_t i = 0; i < outer_count_; ++i)
                {
                    inner_holder_[i]->~inner_holder_t();
                }
                free(inner_holder_);
                free(outer_array_);
            }

            bool contains(const char *key) const
            {
                Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(key));
                size_type inner_index;
                {
                    ReadLock l(&outer_mutex_);
                    inner_index = threaded_rbtree_lower_bound(outer_root_,
                                                              deref_outer_node(),
                                                              user_key,
                                                              deref_outer_key(),
                                                              outer_comparator_);
                }
                size_type key_index;
                {
                    ReadLock l(&inner_holder_[inner_index]->mutex);
                    key_index = threaded_rbtree_equal_unique(inner_holder_[inner_index]->root,
                                                             deref_inner_node(),
                                                             key,
                                                             deref_inner_key(),
                                                             inner_comparator_);
                }
                return key_index != inner_node_t::nil_sentinel;
            }

            void insert(KeyHandle handle)
            {
                size_type inner_node = reinterpret_cast<size_type>(handle);
                Slice user_key = ExtractUserKey(GetLengthPrefixedSlice(deref_inner_key()(inner_node)));
                size_type inner_index;
                size_type outer_count_save;
                {
                    ReadLock l(&outer_mutex_);
                    inner_index = threaded_rbtree_lower_bound(outer_root_,
                                                                   deref_outer_node(),
                                                                   user_key,
                                                                   deref_outer_key(),
                                                                   outer_comparator_);
                    outer_count_save = outer_count_;
                }
                assert(inner_index < outer_root_.get_count());
                size_type version;
                size_type height;
                inner_root_t *inner_root;
                {
                    WriteLock l(&inner_holder_[inner_index]->mutex);
                    version = inner_holder_[inner_index]->version;
                    inner_root = &inner_holder_[inner_index]->root;
                    threaded_rbtree_stack_t<inner_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(*inner_root, stack, deref_inner_node(), inner_node, inner_comparator_);
                    height = stack.height;
                    threaded_rbtree_insert(*inner_root, stack, deref_inner_node(), inner_node);
                }
                if (height < split_max_depth || outer_count_save + 1 == outer_node_t::nil_sentinel)
                {
                    return;
                }
                WriteLock l(&outer_mutex_);
                assert(inner_root->root.root != inner_node_t::nil_sentinel);
                Slice bound = ExtractUserKey(GetLengthPrefixedSlice(deref_inner_key()(inner_root->root.root)));
                int c = outer_comparator_.compare(bound, outer_array_[inner_index].bound);
                assert(c <= 0);
                if (c == 0)
                {
                    return;
                }
                WriteLock l(&inner_holder_[inner_index]->mutex);
                if (version != inner_holder_[inner_index]->version)
                {
                    return;
                }
                if (outer_count_ == outer_count_max_)
                {
                    outer_count_max_ *= 2;
                    inner_holder_ = (inner_holder_t **)realloc(inner_holder_, sizeof(inner_holder_t *) * outer_count_max_);
                    outer_array_ = (outer_element_t *)realloc(outer_array_, sizeof(outer_element_t) * outer_count_max_);
                    outer_comparator_.max_element = &outer_array_[0].bound;
                    *memory_size_ += (sizeof(outer_element_t) + sizeof(inner_root_t)) * outer_count_;
                }
                inner_holder_[outer_count_] = (inner_holder_t *)allocator_->AllocateAligned(sizeof(inner_holder_t));
                new(inner_holder_[outer_count_]) inner_holder_t();
                inner_root_t *split_root = &inner_holder_[outer_count_]->root;
                ++inner_holder_[inner_index]->version;
                inner_holder_[outer_count_]->version = inner_holder_[inner_index]->version;

                deref_inner_node_t deref = deref_inner_node();
                assert(deref(inner_root->root.root).left_is_child());
                assert(deref(inner_root->root.root).right_is_child());
                size_type root_save = inner_root->root.root;
                size_type prev = threaded_rbtree_move_prev(root_save, deref);
                size_type next = threaded_rbtree_move_next(root_save, deref);
                assert(deref(prev).right_is_child());
                assert(deref(next).left_is_child());

                split_root->root.root = deref(root_save).left_get_link();
                deref(split_root->root.root).set_black();
                split_root->root.set_right(prev);
                deref(prev).right_set_link(outer_node_t::nil_sentinel);

                inner_root->root.root = deref(root_save).right_get_link();
                deref(inner_root->root.root).set_black();
                inner_root->root.set_left(next);
                deref(next).left_set_link(outer_node_t::nil_sentinel);
                {
                    threaded_rbtree_stack_t<inner_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(*inner_root, stack, deref, root_save, inner_comparator_);
                    threaded_rbtree_insert(*inner_root, stack, deref, root_save);
                }
                {
                    threaded_rbtree_stack_t<outer_node_t, stack_max_depth> stack;
                    threaded_rbtree_find_path_for_multi(outer_root_, stack, deref_outer_node(), outer_count_, outer_comparator_t());
                    threaded_rbtree_insert(outer_root_, stack, deref_outer_node(), outer_count_);
                }
            }
        };


    public:
        mutable key_set_t key_set_;
        std::atomic_bool immutable_;
        std::atomic_uintptr_t memory_size_;
        const SliceTransform *transform_;

    public:
        explicit TRBTreeRep(const MemTableRep::KeyComparator &compare, Allocator *allocator,
                            const SliceTransform *transform) : MemTableRep(allocator),
                                                               key_set_(compare, allocator, &memory_size_),
                                                               immutable_(false),
                                                               memory_size_(0),
                                                               transform_(transform)
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
        }

        // Like Insert(handle), but may be called concurrent with other calls
        // to InsertConcurrently for other handles
        virtual void InsertConcurrently(KeyHandle handle) {
            key_set_.insert(handle);
        }

        // Returns true iff an entry that compares equal to key is in the list.
        virtual bool Contains(const char *key) const override
        {
            return key_set_.contains(key);
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
            std::string tmp;
            uint64_t start_rank =
                key_set_.approximate_rank(EncodeKey(&tmp, start_ikey));
            uint64_t end_rank =
                key_set_.approximate_rank(EncodeKey(&tmp, end_ikey));
            return (end_rank > start_rank) ? (end_rank - start_rank) : 0;
        }

        virtual void
        Get(const LookupKey &k, void *callback_args,
            bool (*callback_func)(void *arg, const char *entry)) override
        {
            size_type i;
            char const *key;
            {
                ReadLock l(&lock_);
                i = key_set_.lwb_i(k.memtable_key().data());
                if(i == key_set_.end_i())
                {
                    return;
                }
                key = key_set_.key_at(i);
            }
            while(callback_func(callback_args, key))
            {
                ReadLock l(&lock_);
                i = key_set_.next_i(i);
                if(i == key_set_.end_i())
                {
                    return;
                }
                key = key_set_.key_at(i);
            }
        }

        virtual ~TRBTreeRep() override
        {
            key_set_.clear();
        }

        class Iterator : public MemTableRep::Iterator
        {
            key_set_t *tree_;
            size_type where_;
            port::RWMutex &lock_;
            mutable std::string tmp_key_;

            friend class TRBTreeRep;

            Iterator(key_set_t *tree, port::RWMutex &lock) : tree_(tree), where_(tree_->end_i()), lock_(lock)
            {
            }

        public:
            virtual ~Iterator() override{}

            // Returns true iff the iterator is positioned at a valid node.
            virtual bool Valid() const override
            {
              return where_ != tree_->end_i();
            }

            // Returns the key at the current position.
            // REQUIRES: Valid()
            virtual const char *key() const override
            {
                ReadLock l(&lock_);
                return tree_->key_at(where_);
            }

            // Advances to the next position.
            // REQUIRES: Valid()
            virtual void Next() override
            {
                ReadLock l(&lock_);
                where_ = tree_->next_i(where_);
            }

            // Advances to the previous position.
            // REQUIRES: Valid()
            virtual void Prev() override
            {
                ReadLock l(&lock_);
                where_ = tree_->prev_i(where_);
            }

            // Advance to the first entry with a key >= target
            virtual void Seek(const Slice &user_key, const char *memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    ReadLock l(&lock_);
                    where_ = tree_->lwb_i(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    ReadLock l(&lock_);
                    where_ = tree_->lwb_i(tmp_key_.c_str());
                }
            }

            // retreat to the first entry with a key <= target
            virtual void SeekForPrev(const Slice& user_key, const char* memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    ReadLock l(&lock_);
                    where_ = tree_->rlwb_i(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    ReadLock l(&lock_);
                    where_ = tree_->rlwb_i(tmp_key_.c_str());
                }
            }

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToFirst() override
            {
                ReadLock l(&lock_);
                where_ = tree_->beg_i();
            }

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToLast() override
            {
                ReadLock l(&lock_);
                where_ = tree_->rbeg_i();
            }
        };
        class ImmutableIterator : public MemTableRep::Iterator
        {
            key_set_t *tree_;
            size_type where_;
            mutable std::string tmp_key_;

            friend class TRBTreeRep;

            ImmutableIterator(key_set_t *tree) : tree_(tree), where_(tree_->end_i())
            {
            }

        public:
            virtual ~ImmutableIterator() override{}

            // Returns true iff the iterator is positioned at a valid node.
            virtual bool Valid() const override
            {
                return where_ != tree_->end_i();
            }

            // Returns the key at the current position.
            // REQUIRES: Valid()
            virtual const char *key() const override
            {
                return tree_->key_at(where_);
            }

            // Advances to the next position.
            // REQUIRES: Valid()
            virtual void Next() override
            {
                where_ = tree_->next_i(where_);
            }

            // Advances to the previous position.
            // REQUIRES: Valid()
            virtual void Prev() override
            {
                where_ = tree_->prev_i(where_);
            }

            // Advance to the first entry with a key >= target
            virtual void Seek(const Slice &user_key, const char *memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    where_ = tree_->lwb_i(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    where_ = tree_->lwb_i(tmp_key_.c_str());
                }
            }

            // retreat to the first entry with a key <= target
            virtual void SeekForPrev(const Slice& user_key, const char* memtable_key)
            override
            {
                if(memtable_key != nullptr)
                {
                    where_ = tree_->rlwb_i(memtable_key);
                }
                else
                {
                    EncodeKey(&tmp_key_, user_key);
                    where_ = tree_->rlwb_i(tmp_key_.c_str());
                }
            }

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToFirst() override
            {
                where_ = tree_->beg_i();
            }

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToLast() override
            {
                where_ = tree_->rbeg_i();
            }
        };

        virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override
        {
            if(immutable_)
            {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(TRBTreeRep::ImmutableIterator))
                              : operator new(sizeof(TRBTreeRep::ImmutableIterator));
                return new(mem) TRBTreeRep::ImmutableIterator(&key_set_);
            }
            else
            {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(TRBTreeRep::Iterator))
                              : operator new(sizeof(TRBTreeRep::Iterator));
                return new(mem) TRBTreeRep::Iterator(&key_set_, lock_);
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

MemTableRepFactory *NewThreadedRBTreeRepFactory(size_t reserve_size)
{
  return new TRBTreeMemTableRepFactory(reserve_size);
}

} // namespace rocksdb

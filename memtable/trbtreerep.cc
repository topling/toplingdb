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
    class TRBTreeRep : public MemTableRep
    {
        typedef std::size_t size_type;

        struct trb_container_t
        {
            typedef threaded_rb_tree_node_t<uint32_t> node_type;

            struct element_t
            {
                node_type node;
                char const *key;
            };

            std::atomic_uintptr_t *memory_size_ptr;
            size_type default_size;
            size_type capacity;
            size_type size;
            element_t *node;

            trb_container_t(std::atomic_uintptr_t *_size_ptr, size_type _default_size)
                    : memory_size_ptr(_size_ptr), default_size(_default_size), capacity(0), size(0)
            {
            }

            ~trb_container_t()
            {
                clear();
            }

            size_type alloc_one()
            {
                if(size == capacity)
                {
                    if(size == 0)
                    {
                        *memory_size_ptr += default_size * sizeof(element_t);
                        capacity = default_size;
                        node = (element_t *) std::malloc(sizeof(element_t) * default_size);
                    }
                    else
                    {
                        *memory_size_ptr += capacity * sizeof(element_t);
                        capacity *= 2;
                        node = (element_t *) std::realloc(node, sizeof(element_t) * capacity);
                    }
                }
                return size++;
            }

            void clear()
            {
                if(capacity > 0)
                {
                    *memory_size_ptr -= capacity * sizeof(element_t);
                    std::free(node);
                    capacity = 0;
                    size = 0;
                }
            }

            size_type max_size() const
            {
                return node_type::nil_sentinel;
            }
        };

        struct trb_comparator_t
        {
            bool operator()(char const *left, char const *right) const
            {
                return c(left, right) < 0;
            }

            MemTableRep::KeyComparator const &c;
        };

        struct trb_config_t
        {
            typedef char const *key_type;
            typedef char const *const mapped_type;
            typedef char const *const value_type;
            typedef char const *storage_type;
            typedef trb_comparator_t key_compare;

            typedef threaded_rb_tree_node_t<uint32_t> node_type;
            typedef std::false_type unique_type;
            typedef trb_container_t container_type;

            static node_type &get_node(container_type &container, std::size_t index)
            {
                return container.node[index].node;
            }

            static node_type const &get_node(container_type const &container, std::size_t index)
            {
                return container.node[index].node;
            }

            static storage_type &get_value(container_type &container, std::size_t index)
            {
                return container.node[index].key;
            }

            static storage_type const &get_value(container_type const &container, std::size_t index)
            {
                return container.node[index].key;
            }

            static std::size_t alloc_index(container_type &container)
            {
                return container.alloc_one();
            }

            template<class in_type>
            static key_type const &get_key(in_type &&value)
            {
                return value;
            }
        };

    public:
        typedef threaded_rb_tree_impl<trb_config_t> key_set_t;
        key_set_t key_set_;
        mutable port::RWMutex lock_;
        std::atomic_bool immutable_;
        std::atomic_uintptr_t memory_size_;
        const SliceTransform *transform_;

    public:
        explicit TRBTreeRep(size_type reserve_size, const MemTableRep::KeyComparator &compare, MemTableAllocator *allocator,
                            const SliceTransform *transform) : MemTableRep(allocator),
                                                               key_set_({compare}, {&memory_size_, reserve_size}),
                                                               immutable_{false},
                                                               memory_size_{0},
                                                               transform_(transform)
        {
        }

        virtual KeyHandle Allocate(const size_t len, char **buf) override
        {
            *buf = allocator_->Allocate(len);
            return static_cast<KeyHandle>(*buf);
        }

        // Insert key into the list.
        // REQUIRES: nothing that compares equal to key is currently in the list.
        virtual void Insert(KeyHandle handle) override
        {
            WriteLock l(&lock_);
            key_set_.insert(reinterpret_cast<char const *>(handle));
        }

        // Returns true iff an entry that compares equal to key is in the list.
        virtual bool Contains(const char *key) const override
        {
            ReadLock l(&lock_);
            return key_set_.find(key) != key_set_.end();
        }

        virtual void MarkReadOnly() override
        {
            immutable_ = true;
        }

        virtual size_t ApproximateMemoryUsage() override
        {
            return memory_size_.load();
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
                key = key_set_.key(i);
            }
            while(callback_func(callback_args, key))
            {
                ReadLock l(&lock_);
                i = key_set_.next_i(i);
                if(i == key_set_.end_i())
                {
                    return;
                }
                key = key_set_.key(i);
            }
        }

        virtual ~TRBTreeRep() override
        {
            key_set_.clear();
            allocator_->DoneAllocating();
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
                return tree_->key(where_);
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
                return tree_->key(where_);
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
        explicit TRBTreeMemTableRepFactory(std::size_t _reserve_size) : reserve_size(_reserve_size){}

        virtual ~TRBTreeMemTableRepFactory(){}

        virtual MemTableRep *CreateMemTableRep(
                const MemTableRep::KeyComparator &compare, MemTableAllocator *allocator,
                const SliceTransform *transform, Logger *logger) override
        {
          return new TRBTreeRep(reserve_size, compare, allocator, transform);
        }

        virtual const char *Name() const override
        {
          return "TRBTreeMemTableRepFactory";
        }

    private:
        std::size_t reserve_size;
    };
}

MemTableRepFactory *NewThreadedRBTreeRepFactory(size_t reserve_size)
{
  return new TRBTreeMemTableRepFactory(reserve_size);
}

} // namespace rocksdb

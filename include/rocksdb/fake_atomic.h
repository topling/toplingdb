#pragma once
#include <atomic>

template <class T>
class fake_atomic {
  T m_val;
 public:
  fake_atomic() noexcept = default;
  //~fake_atomic() noexcept = default; // not needed
  fake_atomic(const fake_atomic&) = delete;
  fake_atomic& operator=(const fake_atomic&) = delete;
  fake_atomic& operator=(const fake_atomic&) volatile = delete;
  fake_atomic(T val) noexcept : m_val(val) {}

  operator T() const          noexcept { return m_val; }
  operator T() const volatile noexcept { return m_val; }

  T operator=(T x)          noexcept { return m_val = x; }
  T operator=(T x) volatile noexcept { return m_val = x; }

  T operator++(int)          noexcept { return m_val++; }
  T operator++(int) volatile noexcept { return m_val++; }
  T operator--(int)          noexcept { return m_val--; }
  T operator--(int) volatile noexcept { return m_val--; }

  T operator++()          noexcept { return ++m_val; }
  T operator++() volatile noexcept { return ++m_val; }
  T operator--()          noexcept { return --m_val; }
  T operator--() volatile noexcept { return --m_val; }

  T operator+=(T x)          noexcept { return m_val += x; }
  T operator+=(T x) volatile noexcept { return m_val += x; }
  T operator-=(T x)          noexcept { return m_val -= x; }
  T operator-=(T x) volatile noexcept { return m_val -= x; }
  T operator&=(T x)          noexcept { return m_val &= x; }
  T operator&=(T x) volatile noexcept { return m_val &= x; }
  T operator|=(T x)          noexcept { return m_val |= x; }
  T operator|=(T x) volatile noexcept { return m_val |= x; }
  T operator^=(T x)          noexcept { return m_val ^= x; }
  T operator^=(T x) volatile noexcept { return m_val ^= x; }

  bool is_lock_free() const          noexcept { return true; }
  bool is_lock_free() const volatile noexcept { return true; }

  void store(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { m_val = x; }
  void store(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { m_val = x; }

  T load(std::memory_order = std::memory_order_seq_cst) const          noexcept { return m_val; }
  T load(std::memory_order = std::memory_order_seq_cst) const volatile noexcept { return m_val; }

  T exchange(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val = x; return old; }
  T exchange(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val = x; return old; }

  bool compare_exchange_weak  (T& e, T n, std::memory_order = std::memory_order_seq_cst, std::memory_order = std::memory_order_seq_cst)          noexcept { if (m_val == e) { m_val = n; return true; } else { e = m_val; return false; } }
  bool compare_exchange_weak  (T& e, T n, std::memory_order = std::memory_order_seq_cst, std::memory_order = std::memory_order_seq_cst) volatile noexcept { if (m_val == e) { m_val = n; return true; } else { e = m_val; return false; } }
  bool compare_exchange_strong(T& e, T n, std::memory_order = std::memory_order_seq_cst, std::memory_order = std::memory_order_seq_cst)          noexcept { return compare_exchange_weak(e, n); }
  bool compare_exchange_strong(T& e, T n, std::memory_order = std::memory_order_seq_cst, std::memory_order = std::memory_order_seq_cst) volatile noexcept { return compare_exchange_weak(e, n); }

  T fetch_add(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val += x; return old; }
  T fetch_add(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val += x; return old; }
  T fetch_sub(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val -= x; return old; }
  T fetch_sub(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val -= x; return old; }
  T fetch_and(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val &= x; return old; }
  T fetch_and(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val &= x; return old; }
  T fetch_or (T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val |= x; return old; }
  T fetch_or (T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val |= x; return old; }
  T fetch_xor(T x, std::memory_order = std::memory_order_seq_cst)          noexcept { T old = m_val; m_val ^= x; return old; }
  T fetch_xor(T x, std::memory_order = std::memory_order_seq_cst) volatile noexcept { T old = m_val; m_val ^= x; return old; }

#if __cplusplus > 201402L
  static constexpr bool is_always_lock_free = true;
#endif
};

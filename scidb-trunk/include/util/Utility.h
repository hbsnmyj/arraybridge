/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef UTIL_UTILITY_H_
#define UTIL_UTILITY_H_

/****************************************************************************/

#include <ctype.h>                                       // For isspace
#include <string.h>                                      // For strcmp
#include <vector>                                        // For vector
#include <typeinfo>                                      // For typeid
#include <functional>                                    // For less<>.
#include <limits>                                        // For numeric_limits
#include <util/Platform.h>                               // For SCIDB_NORETURN

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 *  @brief      Prevents subclasses from being allocated on the heap.
 *
 *  @details    Class stackonly hides its new operators to prevent it, and any
 *              class that might derive from it, from being directly allocated
 *              on the heap. It mimics similar boost utility classes that seek
 *              to constrain the semantics of a class through inheritance, and
 *              though not entirely foolproof, nevertheless serves as a useful
 *              hint that an object is being initialized for its side effect.
 *
 *              It's useful when implementing the RIIA idiom, where it ensures
 *              that the lifetime of an object is tied to the lexical scope in
 *              which it is instantiated:
 *  @code
 *                  class Lock : stackonly, boost::noncopyable
 *                  {
 *                     Lock(...) ...
 *                    ~Lock(...) ...
 *                  }  lock(...);
 *  @endcode
 *              since without allocating an object on the heap there is no way
 *              for it to escape the current program block.
 *
 *  @see        http://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Requiring_or_Prohibiting_Heap-based_Objects
 *              for more on the underlying idiom.
 *
 *  @see        http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 *              for more on the RIIA pattern.
 *
 *  @see        http://www.boost.org/doc/libs/1_54_0/libs/utility/utility.htm
 *              for boost::noncopyable.
 *
 *  @author     jbell@paradigm4.com
 */
class stackonly
{
            void*             operator new(size_t);
            void*             operator new[](size_t);
            void              operator delete(void*);
            void              operator delete[](void*);
};

/**
 *  @brief      A trivial custom deleter for use with class shared_ptr.
 *
 *  @details    Sometimes it is desirable to create a shared_ptr to an already
 *              existing object so that the shared_ptr does not try to destroy
 *              the object when there are no remaining references. The factory
 *              function:
 *  @code
 *                  std::shared_ptr<X>   newX();
 *  @endcode
 *              might sometimes wish to return a statically allocated instance
 *              for example. The solution is to use a null_deleter:
 *  @code
 *                  std::shared_ptr<X> newX()
 *                  {
 *                      static X x;                      // Must Not delete x
 *
 *                      return std::shared_ptr<X>(&x,null_deleter());
 *                  }
 *  @endcode
 *              This same technique also works for any object that is known to
 *              outlive the shared_ptr that is aimed at it.
 *
 *  @see        http://www.boost.org/doc/libs/1_55_0/libs/smart_ptr/sp_techniques.html
 *              for use of class shared_ptr with statically allocated objects.
 *
 *  @author     jbell@paradigm4.com
 */
struct null_deleter
{
            void              operator()(const void*) const {}
};

/**
 *  @brief      Implements a pair that is ordered by its first component only.
 *
 *  @details    Class Keyed implements a simple key-value pair as an aggregate
 *              that is totally ordered by the first element of the pair only.
 *              Contrast this with class std::pair,  which tests both elements
 *              in its comparison operator,  and whose constructors prevent it
 *              from being created within the initializer list of an array.
 *
 *              Class Keyed is useful for implementing 'flat' maps:
 *  @code
 *                  static const Keyed<const char*,int,less_strcmp> m[] =
 *                  {
 *                      {"apr",4},
 *                      {"aug",8},
 *                         ..
 *                      {"sep",9},
 *                  };
 *
 *                  ...  = std::equal_range(m,m+SCIDB_SIZE(m),"mar");
 *  @endcode
 *              Here lower_bound() returns a pair of iterators - pointers - to
 *              the key-value pair matching the entry for the month of March.
 *
 *  @author     jbell@paradigm4.com
 */
template< class K, class V, class C = std::less<K> >
struct Keyed
{
 const  K    key;                                        // The sort key
 const  V    value;                                      // The keyed value
 friend bool operator<(const K& a,const Keyed<K,V,C>& b) {return C()(a,b.key);}
 friend bool operator<(const Keyed<K,V,C>& a,const K& b) {return C()(a.key,b);}
};

/**
 *  Wraps the standard string comparison function as a function object that is
 *  suitable for use as the comparison for a standard associative container.
 */
struct less_strcasecmp : std::binary_function<const char*,const char*,int>
{
    bool operator()(const char* a,const char* b) const   {return strcasecmp(a,b) < 0;}
};

/**
 *  Wraps the standard string comparison function as a function object that is
 *  suitable for use as the comparison for a standard associative container.
 */
struct less_strcmp : std::binary_function<const char*,const char*,int>
{
    bool operator()(const char* a,const char* b) const   {return strcmp(a,b) < 0;}
};

/**
 *  Cast the pointer 'pb' from type 'base*' to type 'derived'. Equivalent to a
 *  static cast in a release build, but has the advantage that the downcast is
 *  checked with an assertion in a debug build.
 */
template<class derived,class base>
inline derived downcast(base* pb)
{
    assert(dynamic_cast<derived>(pb) == pb);             // Is this cast safe?

    return static_cast<derived>(pb);                     // So cast it already
}

/**
 *  Cast the pointer 'pb' from type 'base*' to type 'derived', and assert - or
 *  throw - if the cast fails.
 *
 * @throws SystemException if the cast fails (abort()s in a Debug build)
 */
template<class derived,class base>
inline derived safe_dynamic_cast(base* pb)
{
    void bad_dynamic_cast(const std::type_info&,const std::type_info&) SCIDB_NORETURN;

    if (pb == NULL)                                      // Is the source null
    {
        return NULL;                                     // ...so cast is safe
    }

    if (derived pd = dynamic_cast<derived>(pb))          // Cast succeeded ok?
    {
        return pd;                                       // ...derived pointer
    }

    bad_dynamic_cast(typeid(base),typeid(derived));      // Report the failure
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
/**
 * Cast an integral type to a smaller sized type, ensuring that its
 * value is preserved.
 */
template <typename TO_TYPE, typename FROM_TYPE>
inline TO_TYPE safe_static_cast(FROM_TYPE from)
{
    // There is a penalty for using this conversion, so just use
    // the type without casting back to itself.
    static_assert(!std::is_same<TO_TYPE,FROM_TYPE>::value,
                  "types should be different");
    static_assert(std::numeric_limits<FROM_TYPE>::is_integer
                  && std::numeric_limits<TO_TYPE>::is_integer,
                  "Use boost::numeric_cast<> for floating point.");

    /// Use 'c++filt -t' to translate the types reported by bad_static_cast
    void bad_static_cast(const std::type_info&,const std::type_info&) SCIDB_NORETURN;
    //
    // Static numeric_limits definitions are used so that the number of comparisons
    // performed for the conversions is kept to a bare minimum when the templatized
    // function is instantiated. This keeps the performance penalty to a minimum.
    //
    // Both are signed or both are unsigned
    //
    if (std::numeric_limits<TO_TYPE>::is_signed == std::numeric_limits<FROM_TYPE>::is_signed) {
        // TO_TYPE has smaller bit length to represent the number
        if (std::numeric_limits<TO_TYPE>::digits < std::numeric_limits<FROM_TYPE>::digits) {
            if (from > std::numeric_limits<TO_TYPE>::max()
                || from < std::numeric_limits<TO_TYPE>::min()) {
                bad_static_cast(typeid(TO_TYPE),typeid(FROM_TYPE));
            }
        }
        // else {
        // if FROM_TYPE and TO_TYPE both (un)signed  values and TO_TYPE has
        // a larger (or same) bit length representation then FROM_TYPE set is a
        // proper subset (or equal set) of TO_TYPE set:
        // FROM_TYPE: [ -2^(n-1), 2^(n-1) ) or [0, 2^n )
        // TO_TYPE :  [ -2^(m+n-1), 2^(m+n-1) ) or [0, 2^(n+m) )
        // m >= 0
        // }
    }
    //
    // converting from unsigned to signed
    //
    else if (std::numeric_limits<TO_TYPE>::is_signed) {
        // TO_TYPE has smaller bit length to represent the number
        if (std::numeric_limits<TO_TYPE>::digits < std::numeric_limits<FROM_TYPE>::digits) {
            // [0, 2^(n)] --> [-2^(m-1), 2^(m-1)) ; m < n
            // NOTE: the entire set of TO_TYPE fits into the FROM_TYPE set
            // so the conversions in the comparison below are valid
            if (from > std::numeric_limits<TO_TYPE>::max()) {
                bad_static_cast(typeid(TO_TYPE),typeid(FROM_TYPE));
            }
        }
        else if (std::numeric_limits<TO_TYPE>::digits == std::numeric_limits<FROM_TYPE>::digits) {
            // [0, 2^n]  ---> [ -2^(n-1), 2^(n-1) )
            if (from > std::numeric_limits<TO_TYPE>::max()) {
                bad_static_cast(typeid(TO_TYPE),typeid(FROM_TYPE));
            }
        }
        // else {
            // an unsigned [0, 2^n) can always be represented in the
            // range [ -2^(n+m-1), 2^(n+m-1) ) where m is positive: n,m are in [8, 16,32, 64]
        // }
    }
    //
    // converting from signed to unsigned
    //
    else {
        // [ -2^(n-1), 2^(n-1) ) --> [0, 2^m] where m < n
        if (std::numeric_limits<TO_TYPE>::digits < std::numeric_limits<FROM_TYPE>::digits) {
            if (from < 0 || from > std::numeric_limits<TO_TYPE>::max() ) {
                // This comparison conversion is okay because [0, 2^m) will fit into
                // [ -2^(n-1), 2^(n-1) ) for all m < n
                bad_static_cast(typeid(TO_TYPE),typeid(FROM_TYPE));
            }
        }
        // for a x in [ -2^(n-1), 2^(n-1) ) --> x is in [0, 2^m] for all x > 0, m>=n
        else {
            if (from < 0) {
                bad_static_cast(typeid(TO_TYPE),typeid(FROM_TYPE));
            }
        }
    }
    return static_cast<TO_TYPE>(from);
}
#pragma GCC diagnostic pop

/**
 *  Return true if the truth of 'a' logically implies the truth of 'b'; that
 *  is, 'b' is true whenever 'a' is true.
 */
inline bool implies(bool a,bool b)
{
    return !a || b;                                      // 'a' => 'b
}

/**
 *  Return true if the truth of 'a' logically implies the truth of 'b' and so
 *  also vice versa; in other words, both 'a' and 'b' have precisely the same
 *  truth value.
 */
inline bool iff(bool a,bool b)
{
    return a == b;                                       // 'a' <=> 'b'
}

/**
 *  Return true if the integer 'n' is a power of two. This is the case exactly
 *  when the bit representation for 'n' has precisely one bit set.
 *
 *  @see http://en.wikipedia.org/wiki/Bit_manipulation
 */
inline bool isPowerOfTwo(size_t n)
{
#if defined (__GNUC__)

    return __builtin_popcountl(n) == 1;                  // Just one bit set?

#endif

    return n!=0 && (n & (n - 1))==0;                     // Just one bit set?
}


/**
 *  @brief      Determine the count of trailing zero in the binary
 *              representation of a number.
 *
 *  @details    The value parameter is the number for which the
 *              count of trailing zeros is to be determined.
 *              Example, lets take the number 32. In binary:
  *             00000000 00000000 00000000 00100000
 *              Notice there are five zeros to the right of the
 *              rightmost 1 bit.  Therefore, this routine will return 5.
 *
 *  @param      value  The value to use for the determination
 *
 *  @return     # of zero bits in the 64 bit word.
 *
 *  @author     mcorbett@paradigm4.com
 *
 */
size_t getTrailingZeros(size_t in_value);  // See Utility.cpp for non-GNUC version

#if defined (__GNUC__)
inline size_t getTrailingZeros(size_t value)
{
    if (0UL == value)  return 64;
    return static_cast<size_t>( __builtin_ctzl(value));  // O(1)
}
#endif



/**
 *  @brief      Backward compatibility interface to the One True TSV Parser.
 *
 *  @details    This function parses lines of 'tab separated values' text.  It
 *              modifies the input line in-place to unescape TSV escape chars,
 *              and returns a  vector of pointers to  individual fields in the
 *              modified line.
 *
 *              The only possible error is caused by a backslash tab sequence,
 *              which is illegal.
 *
 *  @note       If you choose a field delimiter other than TAB (ascii 0x09) be
 *              certain that the data columns do not contain that character or
 *              you will get unexpected results.  Use of non-TAB delimiters is
 *              discouraged for this reason.
 *
 *  @note       You may prefer to use the @c TsvParser class for field-by-field
 *              parsing rather than line-at-a-time parsing.  This function is
 *              implemented using @c TsvParser .
 *
 *  @param      line    modifiable null-terminated input line
 *
 *  @param      fields  vector of pointers into the modified line
 *
 *  @param      delim   alternate field delimiter
 *
 *  @return     true iff line was successfully parsed
 *
 *  @see        scidb::TsvParser
 *  @see        http://dataprotocols.org/linear-tsv/
 *
 *  @author     mjl@paradigm4.com
 *
 */
bool tsv_parse(char* line,std::vector<char*>& fields,char delim = '\t');

/**
 *  @brief      Match an integer, floating point number, or 'nan'
 *
 *  @note       Leading and trailing whitespace in @c val are ignored.
 *
 *  @param      val     a NULL-terminated string to search for a number
 *
 *  @return     true iff an integer, floating point number, or 'nan' is found
 *
 *  @throws     runtime_error on regex compilation failure (should not happen)
 *
 *  @note       The initial call compiles a regular expression and thus is not
 *              thread-safe. To get thread safety call once from main() before
 *              launching threads. Subsequent calls are fine.
 *
 *  @author     mjl@paradigm4.com
 */
bool isnumber(const char* val);

/**
 *  @brief      Test string for whitespaciness
 *
 *  @param      val     a NULL-terminated string to test
 *  @return     true iff argument string is all whitespace
 */
inline bool iswhitespace(const char* val)
{
    while (::isspace(*val)) {
        ++val;
    }
    return *val == '\0';
}

/**
 * Initialize a region of memory to zero when !defined(NDEBUG)
 * @note valgrind complains about uninitialized alignment padding in various struct's
 *       which are treated as contiguous memory buffers. This function is handy to
 *       suppress such complaints.
 * @param ptr pointer to the memory region
 * @param size memory region size
 */
inline void setToZeroInDebug(void * ptr, size_t size)
{
    if (isDebug()) {
        ::memset(ptr, 0, size);
    }
}

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/

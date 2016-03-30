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

/*
 * NewUsageOfArena.h demonstrates our proposed usages of Arena.
 * See code and comment between "BEGIN demonstration" and "END demonstration".
 *
 * @author Donghui Zhang
 */

#ifndef NEW_USAGE_OF_ARENA_H_
#define NEW_USAGE_OF_ARENA_H_

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestAssert.h>
#include <util/Arena.h>

using std::shared_ptr;
using std::unique_ptr;
using std::scoped_allocator_adaptor;
using std::vector;
using std::list;
using std::map;
using std::string;

namespace scidb {

/// The second template type of a unique_ptr.
/// c++14 does not have allocate_unique. So we provide our own.
template<class T, class Alloc>
struct Deleter {
    explicit Deleter(const Alloc& alloc)
      : alloc_(alloc)
    {}

    void operator()(T*const p)
    {
        std::allocator_traits<Alloc>::destroy(alloc_, p);
        std::allocator_traits<Alloc>::deallocate(alloc_, p, 1);
    }

private:
    Alloc alloc_;
};

/// @return a unique_ptr allocated from the allocator from argument.
template<class T, class Alloc, class... Args>
std::unique_ptr<T, Deleter<T, Alloc>> allocate_unique(
    const Alloc& _alloc, Args&&... args)
{
    Alloc alloc(_alloc);
    T*const p = alloc.allocate(1);
    std::allocator_traits<Alloc>::construct(alloc, p, std::forward<Args>(args)...);
    return std::unique_ptr<T, Deleter<T, Alloc>>(p, Deleter<T, Alloc>(alloc));
}

//////////////////////////
// BEGIN demonstration. //
//////////////////////////

//
// DEMO: raw allocation.
//
void DemoRawAllocation(const arena::ArenaPtr& a)
{
    constexpr size_t kNumBytes = 10;
    void* block = a->malloc(kNumBytes);
    a->free(block, kNumBytes);
}

//
// DEMO: arena-aware new.
//
struct S
{
    double d;
    explicit S(double _d = 0) noexcept
      : d(_d)
    {}
};

void DemoArenaAwareNew(const arena::ArenaPtr& a)
{
    // Scalar version.
    S* s = arena::newScalar<S>(*a, 3.14);
    arena::destroy(*a, s);

    // Array version.
    S* vec = arena::newVector<S>(*a, 10);
    vec[9].d = 3.14;
    arena::destroy(*a, vec);
}

//
// DEMO: smart pointers.
//
void DemoSmartPointers(const arena::ArenaPtr& a)
{
    using Alloc = arena::Allocator<S>;
    Alloc alloc(a);
    std::shared_ptr<S> ss = std::allocate_shared<S, Alloc>(alloc, 3.14);
    std::unique_ptr<S, Deleter<S, Alloc>> us = allocate_unique<S, Alloc>(alloc, 3.14);
}

//
// DEMO: STL containers.
//
template<class T>
using Vector = std::vector<T, arena::Allocator<T>>;

void DemoSTLContainers(const arena::ArenaPtr& a)
{
    arena::Allocator<S> alloc(a);
    Vector<S> v(10, alloc);
    v[9].d = 3.14;
}

//
// DEMO: multi-level containers.
// The idea is to provide the outermost container a scoped_allocator_adaptor.
// Also, you may use std::uses_allocator to ensure even a container inside a struct may be part of the chain.
//
// The example below shows a three-level container:
// VectorOfStruct --> StructOfMap --> Map
//
using Map = std::map<int, S, std::less<int>, arena::Allocator<std::pair<const int, S>>>;

struct StructOfMap
{
    Map my_map;

    explicit StructOfMap(const arena::Allocator<Map::value_type>& alloc) noexcept
      : my_map(alloc)
    {}

    // move constructor with alloc.
    StructOfMap(StructOfMap&& other, const arena::Allocator<Map::value_type>& alloc) noexcept
      : my_map(std::move(other.my_map), alloc)
    {}
};

}  // namespace scidb

namespace std
{
// This overloading enables StructOfMap to pass a scoped allocator to its data member.
template<class A>
struct uses_allocator<scidb::StructOfMap, A>
{
    static constexpr bool value = true;
};
}  // namespace std

namespace scidb
{
using AllocatorForVectorOfStruct = std::scoped_allocator_adaptor<arena::Allocator<StructOfMap>>;
using VectorOfStruct = std::vector<StructOfMap, AllocatorForVectorOfStruct>;

void DemoMultiLevelContainers(const arena::ArenaPtr& a)
{
    VectorOfStruct vec(10, AllocatorForVectorOfStruct(arena::Allocator<StructOfMap>(a)));
    StructOfMap elem(vec.get_allocator().inner_allocator());
    vec.push_back(std::move(elem));
}

////////////////////////
// END demonstration. //
////////////////////////

/// Test driver that calls the above demonstration functions.
class NewUsageOfArena: public CppUnit::TestFixture
{
    /// Whether the unit test should print anything.
    static constexpr bool _debug = true;

public:
    CPPUNIT_TEST_SUITE (NewUsageOfArena);
    CPPUNIT_TEST (TestMain);
    CPPUNIT_TEST_SUITE_END();

public:
    /// Call the demo functions, printing allocated memory before/after each function.
    void TestMain() {
        // Generate a Lea Arena as a child of the root arena.
        arena::ArenaPtr a = arena::newArena(arena::Options("NewUsageOfArena").lea(arena::getArena()));

        // Prepare a vector of functions.
        using NameAndFunction = std::pair<string, std::function<void(const arena::ArenaPtr&)>>;
        std::vector<NameAndFunction> functions;
        functions.push_back(NameAndFunction("DemoRawAllocation", DemoRawAllocation));
        functions.push_back(NameAndFunction("DemoArenaAwareNew", DemoArenaAwareNew));
        functions.push_back(NameAndFunction("DemoSmartPointers", DemoSmartPointers));
        functions.push_back(NameAndFunction("DemoSTLContainers", DemoSTLContainers));
        functions.push_back(NameAndFunction("DemoMultiLevelContainers", DemoMultiLevelContainers));

        // Run each function.
        for (auto it=functions.begin(); it!=functions.end(); ++it) {
            if (_debug) {
                std::cout << it->first << ": before size = " << a->allocated();
            }
            it->second(a);
            if (_debug) {
                std::cout << ", after size = " << a->allocated() << std::endl;
            }
        }

        // Reset the arena.
        a->reset();
    }
};  // class NewUsageOfArena

}  // namespace scidb

CPPUNIT_TEST_SUITE_REGISTRATION(scidb::NewUsageOfArena);

#endif  // NEW_USAGE_OF_ARENA_H_

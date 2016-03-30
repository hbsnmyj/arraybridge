/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2015 SciDB, Inc.
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
 * @author poliocough@gmail.com
 */

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <vector>
#include <stdlib.h>

#include <boost/lexical_cast.hpp>
#include <boost/assign.hpp>
#include <boost/algorithm/string.hpp>

#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "system/ErrorsLibrary.h"

using namespace std;
using namespace boost;
using namespace boost::assign;
using namespace scidb;

//in some rare cases, on older versions, string values are not null-terminated; we aim to be nice
string get_null_terminated_string(char const* input, size_t const size)
{
    if(size == 0)
    {
        return string("");
    }
    else if (input[size-1] != 0)
    {
        return string(input, size);
    }
    else
    {
        return string(input);
    }
}

enum conversion_type
{
    INTEGER = 0,
    UINT64 = 1,
    DOUBLE = 2,
    BOOL   = 3
};

/**
 * DCAST: cast with default, does not throw an error.
 * Tries to cast input type S to the appropriate T type.
 * If the cast fails, returns the supplied default.
 * @parameter args two element array of Value pointers,
 *             element 0 contains the string value to be converted into type S
 *             element 1 contains the default value in case of cast failure
 * @parameter res pointer to the result Value
 */
template <typename T, typename S, conversion_type C>
static void dcast (const Value** args, Value* res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    const char* start = args[0]->getString();
    char* end = const_cast<char*> (start);
    S val;
    bool error = false;
    if(C == INTEGER)
    {
        errno = 0;
        val = static_cast<S>(strtoll(start,  &end, 10));
        error = (errno != 0);
    }
    else if (C == DOUBLE)
    {
        errno = 0;
        val = static_cast<S>(strtold(start, &end));
        error = (errno != 0);
    }
    else if (C == UINT64)
    {
        //a case-specific coockalacka:
        //strtoull will accept '-1' and happily convert it to a big number. We aim to avoid that.
        //the solution is to quickly reject the conversion if the string contains a '-' anywhere
        size_t i = 0;
        while(start[i] != '\0')
        {
            if(start[i] == '-')
            {
                error = true;
                break;
            }
            ++i;
        }
        if(!error)
        {
            errno = 0;
            val = static_cast<S>(strtoull(start, &end, 10));
            error = (errno != 0);
        }
    }
    else //BOOL: accepts 0,F,N,NO,FALSE, or 1,T,Y,YES,TRUE (ignore case)
    {
        string str(start);
        trim(str);
        to_lower(str);
        if ( str == "0" || str == "f" || str == "n" || str == "no" || str == "false")
        {
            res->set<bool>(false);
            return;
        }
        else if (str == "1" || str == "t" || str == "y" || str == "yes" || str == "true")
        {
            res->set<bool>(true);
            return;
        }
        error = true;
        val   = false;
    }
    while( isspace(*end) )
    {   ++end; }
    T const min = std::numeric_limits<T>::min();
    T const max = std::numeric_limits<T>::max();
    if ( error          ||
         *start == '\0' ||
         *end   != '\0' ||
         ((C == INTEGER) && val < min) ||
         ((C == INTEGER) && val > max) )
    {
        Value const* def = args[1];
        if(def->isNull())
        {
            res->setNull( def->getMissingReason() );
        }
        else
        {
            res->set<T>( def->get<T>() );
        }
    }
    else
    {
        res->set<T>(static_cast<T>(val));
    }
}

static scidb::UserDefinedFunction dcast_double (scidb::FunctionDescription("dcast", list_of("string")("double"),  "double", &dcast<double,   double,   DOUBLE> ));
static scidb::UserDefinedFunction dcast_float  (scidb::FunctionDescription("dcast", list_of("string")("float"),   "float",  &dcast<float,    double,   DOUBLE>  ));
static scidb::UserDefinedFunction dcast_bool   (scidb::FunctionDescription("dcast", list_of("string")("bool"),    "bool",   &dcast<bool,     bool,     BOOL>    ));
static scidb::UserDefinedFunction dcast_int64  (scidb::FunctionDescription("dcast", list_of("string")("int64"),   "int64" , &dcast<int64_t,  int64_t,  INTEGER> ));
static scidb::UserDefinedFunction dcast_int32  (scidb::FunctionDescription("dcast", list_of("string")("int32"),   "int32",  &dcast<int32_t,  int64_t,  INTEGER> ));
static scidb::UserDefinedFunction dcast_int16  (scidb::FunctionDescription("dcast", list_of("string")("int16"),   "int16",  &dcast<int16_t,  int64_t,  INTEGER> ));
static scidb::UserDefinedFunction dcast_uint64 (scidb::FunctionDescription("dcast", list_of("string")("uint64"),  "uint64", &dcast<uint64_t, uint64_t, UINT64> ));
static scidb::UserDefinedFunction dcast_uint32 (scidb::FunctionDescription("dcast", list_of("string")("uint32"),  "uint32", &dcast<uint32_t, int64_t,  INTEGER> ));
static scidb::UserDefinedFunction dcast_uint16 (scidb::FunctionDescription("dcast", list_of("string")("uint16"),  "uint16", &dcast<uint16_t, int64_t,  INTEGER> ));
static scidb::UserDefinedFunction dcast_uint8  (scidb::FunctionDescription("dcast", list_of("string")("uint8"),   "uint8",  &dcast<uint8_t,  int64_t,  INTEGER>  ));
static scidb::UserDefinedFunction dcast_int8   (scidb::FunctionDescription("dcast", list_of("string")("int8"),    "int8",   &dcast<int8_t,   int64_t,  INTEGER>  ));
// XXX To add a datetime conversion here, need to find a routine that does it, and/or replicate what parseDateTime (TypeSystem.cpp) does

template <bool trim_characters_supplied>
static void trim (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string characters = " ";
    if(trim_characters_supplied)
    {
        if(args[1]->isNull())
        {
            res->setNull(0);
            return;
        }
        characters = get_null_terminated_string(args[1]->getString(), args[1]->size());
    }
    string input = get_null_terminated_string(args[0]->getString(), args[0]->size());
    trim_if(input, is_any_of(characters));
    res->setString(input);
}

static scidb::UserDefinedFunction trim_space (scidb::FunctionDescription("trim", list_of("string"),           "string", &trim<false> ));
static scidb::UserDefinedFunction trim_str   (scidb::FunctionDescription("trim", list_of("string")("string"), "string", &trim<true> ));

static void int_to_char (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
      res->setNull(args[0]->getMissingReason());
      return;
    }
    uint8_t input = args[0]->getUint8();
    res->setChar(input);
}

static scidb::UserDefinedFunction int_to_c (scidb::FunctionDescription("int_to_char", list_of("uint8"), "char", &int_to_char ));
static scidb::UserDefinedFunction c_to_int (scidb::FunctionDescription("char_to_int", list_of("char"), "uint8", &int_to_char ));

static void codify (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
      res->setNull(args[0]->getMissingReason());
      return;
    }
    const char* input = args[0]->getString();
    size_t inputLen = args[0]->size();
    ostringstream out;
    for (size_t i=0; i< inputLen; ++i)
    {
        char c = input[i];
        int32_t res = c;
        out<<res<<"|";
    }
    res->setString(out.str().c_str());
}

static scidb::UserDefinedFunction asciify_str (scidb::FunctionDescription("codify", list_of("string"), "string", &codify ));

static void keyed_value( const Value** args, Value *res, void* )
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    else if (args[1]->isNull())
    {
        res->setNull(1);
        return;
    }
    string cell =        get_null_terminated_string(args[0]->getString(), args[0]->size());
    string info_field =  get_null_terminated_string(args[1]->getString(), args[1]->size());
    vector<string> values;
    split(values, cell, is_from_range(';', ';'));
    for(size_t i=0, n=values.size(); i<n; ++i)
    {
        vector<string> pair;
        split(pair, values[i], is_from_range('=','='));
        if(pair.size()!=2)
        {
            res->setNull(2);
            return;
        }
        if(pair[0]==info_field)
        {
            res->setString(pair[1]);
            return;
        }
    }
    (*res) = (*args[2]);
}
static scidb::UserDefinedFunction key_value_extract( scidb::FunctionDescription("keyed_value", list_of("string")("string")("string"), "string", &keyed_value));

void char_count(const scidb::Value** args, scidb::Value* res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string input =      get_null_terminated_string(args[0]->getString(), args[0]->size());
    if(args[1]->isNull())
    {
        res->setNull(0);
        return;
    }
    string separator =  get_null_terminated_string(args[1]->getString(), args[1]->size());
    if (separator.size() == 0)
    {
        res->setNull(1);
        return;
    }
    size_t sepSize = separator.size();
    uint32_t count = 0;
    for (size_t i =0, s=input.size(); i<s; ++i) //XXX: is there a boost any_of splitter for this? One that does not require splitting?
    {
        for(size_t j=0; j<sepSize; ++j)
        {
            if(input[i] == separator[j])
            {
                ++count;
                break;
            }
        }
    }
    res -> setUint32(count);
}
static scidb::UserDefinedFunction ntdv( scidb::FunctionDescription("char_count", list_of("string")("string"), "uint32", &char_count));

template <bool custom_separator>
void nth_tdv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    if (args[1]->isNull())
    {
        res->setNull(0);
        return;
    }
    string separator = ",";
    if(custom_separator)
    {
       if(args[2]->isNull())
       {
           res->setNull(0);
           return;
       }
       separator = get_null_terminated_string(args[2]->getString(), args[2]->size());
       if(separator.size()==0)
       {
           res->setNull(0);
           return;
       }
    }
    uint32_t n = args[1]->getUint32();
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    split(values, cell, is_any_of(separator));
    if (values.size() <= n)
    {
        res->setNull(0);
        return;
    }
    res->setString(values[n]);
}
static scidb::UserDefinedFunction ntcsv( scidb::FunctionDescription("nth_csv", list_of("string")("uint32"),           "string", &nth_tdv<false>));
static scidb::UserDefinedFunction nttdv( scidb::FunctionDescription("nth_tdv", list_of("string")("uint32")("string"), "string", &nth_tdv<true>));

template <bool custom_separator>
void maxlen_tdv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    string separator = ",";
    if(custom_separator)
    {
       if(args[1]->isNull())
       {
           res->setNull(0);
           return;
       }
       separator = get_null_terminated_string(args[1]->getString(), args[1]->size());
       if(separator.size()==0)
       {
           res->setNull(0);
           return;
       }
    }
    split(values, cell, is_any_of(separator));
    size_t maxSize =0;
    for(size_t i=0, n=values.size(); i<n; ++i)
    {
        if(values[i].size()> maxSize)
            maxSize=values[i].size();
    }
    res->setUint64(maxSize);
}

static scidb::UserDefinedFunction mlcsv( scidb::FunctionDescription("maxlen_csv", list_of("string"),           "uint32", &maxlen_tdv<false>));
static scidb::UserDefinedFunction mltdv( scidb::FunctionDescription("maxlen_tdv", list_of("string")("string"), "uint32", &maxlen_tdv<true>));

/**
 * arg0: FORMAT FIELD
 * arg1: sample FIELD
 * arg2: attribute name
 */
static void extract_format_field( const Value **args, Value* res, void*) {
    for(int i = 0; i < 3; i++) { 
        if(args[i]->isNull()) { 
            res->setNull(args[i]->getMissingReason());
            return;
        }
    }

    const char* formatField = args[0]->getString();
    size_t formatLen = args[0]->size();
    const char* sampleField = args[1]->getString();
    size_t sampleLen = args[1]->size();
    const char* attrName = args[2]->getString();
    size_t attrLen = args[2]->size();

    size_t index = 0;
    size_t j = 0, k = 0;
    bool match = false;
    for(; j < formatLen && !match; j += k) { 
      if(formatField[j] == ':') { index++; j++; }
      match = true;
      for(k = 0; j+k < formatLen && formatField[j+k] != ':'; k++) { 
        if(k >= attrLen || formatField[j+k] != attrName[k]) { match = false; }
      }
    }

    if(!match) { 
      res->setNull(0);
      return;
    }

    size_t start = 0;
    size_t indexi = 0;
    for(; start < sampleLen && indexi < index; start++) { 
        if(sampleField[start] == ':') { 
            indexi += 1;
        }
    }
    
    size_t end = start+1;
    for(; end < sampleLen && sampleField[end] != ':'; end += 1) 
        {}

    size_t size = end - start + 1;
    res->setSize(size);
    memcpy(res->data(), &sampleField[start], (end-start));
    ((char*)res->data())[size-1]=0;
}
static scidb::UserDefinedFunction format_extract(scidb::FunctionDescription("format_extract", list_of("string")("string")("string"), "string", &extract_format_field));

void toss(const scidb::Value** args, scidb::Value* res, void*)
{
    string error;
    if(args[0]->isNull())
    {
        error = "null";
    }
    else
    {
        error = get_null_terminated_string(args[0]->getString(), args[0]->size());
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error;
}
static scidb::UserDefinedFunction errortoss( scidb::FunctionDescription("throw", list_of("string"), "uint8", &toss));

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

/**
 * @file Exceptions.h
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Exceptions which thrown inside SciDB
 */

#ifndef EXCEPTIONS_H_
#define EXCEPTIONS_H_

#include <stdint.h>
#include <iostream>
#include <sstream>
#include <string.h>
#include <string>
#include <memory>
#include <boost/format.hpp>

#include <query/QueryID.h>
#include <system/ErrorCodes.h>
#include <system/Constants.h>
#include <util/Platform.h>
#include <util/StringUtil.h>

#define SYSTEM_EXCEPTION(short_error_code, long_error_code)\
    scidb::SystemException(REL_FILE, __FUNCTION__, __LINE__, "scidb", short_error_code,\
    long_error_code, #short_error_code, #long_error_code)

#define SYSTEM_EXCEPTION_SPTR(short_error_code, long_error_code)        \
    std::make_shared<scidb::SystemException>(                         \
        REL_FILE, __FUNCTION__, __LINE__, "scidb",                      \
        int(short_error_code), int(long_error_code),                    \
        #short_error_code, #long_error_code)

#define USER_EXCEPTION(short_error_code, long_error_code)\
    scidb::UserException(REL_FILE, __FUNCTION__, __LINE__, "scidb", short_error_code,\
    long_error_code, #short_error_code, #long_error_code)

#define USER_EXCEPTION_SPTR(short_error_code, long_error_code)  \
    std::make_shared<scidb::UserException>(                   \
        REL_FILE, __FUNCTION__, __LINE__, "scidb",              \
        int(short_error_code), int(long_error_code),            \
        #short_error_code, #long_error_code)

#define USER_QUERY_EXCEPTION(short_error_code, long_error_code, parsing_context)\
    scidb::UserQueryException(REL_FILE, __FUNCTION__, __LINE__, "scidb", short_error_code,\
        long_error_code, #short_error_code, #long_error_code, parsing_context)

#define CONV_TO_USER_QUERY_EXCEPTION(exception, parsing_context)\
    scidb::UserQueryException(exception.getFile().c_str(), exception.getFunction().c_str(), exception.getLine(),\
        exception.getErrorsNamespace().c_str(), exception.getShortErrorCode(), exception.getLongErrorCode(),\
        exception.what(), exception.getStringifiedShortErrorCode().c_str(),\
        exception.getStringifiedLongErrorCode().c_str(), parsing_context, exception.getQueryId())

#define USER_QUERY_EXCEPTION_SPTR(short_error_code, long_error_code, parsing_context) \
    std::make_shared<scidb::UserQueryException>(                      \
        REL_FILE, __FUNCTION__, __LINE__, "scidb",                      \
        int(short_error_code), int(long_error_code),                    \
        #short_error_code, #long_error_code, parsing_context)

#define PLUGIN_SYSTEM_EXCEPTION(error_namespace, short_error_code, long_error_code)\
    scidb::SystemException(REL_FILE, __FUNCTION__, __LINE__, error_namespace, short_error_code,\
    long_error_code, #short_error_code, #long_error_code)

#define PLUGIN_USER_EXCEPTION(error_namespace, short_error_code, long_error_code)\
    scidb::UserException(REL_FILE, __FUNCTION__, __LINE__, error_namespace, short_error_code,\
    long_error_code, #short_error_code, #long_error_code)

#define PLUGIN_USER_QUERY_EXCEPTION(error_namespace, short_error_code, long_error_code, parsing_context)\
    scidb::UserQueryException(REL_FILE, __FUNCTION__, __LINE__, error_namespace, short_error_code,\
        long_error_code, #short_error_code, #long_error_code, parsing_context)

/**
 * This macro is equivalent to an assertion in DEBUG build, and an
 * exception in RELEASE build.
 */
#ifndef NDEBUG
#   define ASSERT_EXCEPTION(_cond_, _msg_)              \
    do {                                                \
        assert(_cond_);                                 \
        std::string m(_msg_); /* avoid warning */       \
    } while (0)
#else
#   define ASSERT_EXCEPTION(_cond_, _msg_)                              \
    do {                                                                \
        if (! static_cast<bool>(_cond_)) {                              \
            throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL,            \
                                   scidb::SCIDB_LE_ASSERTION_FAILED)    \
                << #_cond_ << __FILE__ << __LINE__ << _msg_;            \
        }                                                               \
    } while (0)
#endif

/**
 * The macro is equivalent to ASSERT_EXCEPTION( false, exceptionMsg );  But it is
 * designed so that it will not generate compiler warning messages if it is the
 * only action is a non-void function.
 */
#define ASSERT_EXCEPTION_FALSE(_msg_) \
    assert(false); \
    throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL, scidb::SCIDB_LE_UNREACHABLE_CODE) << _msg_;

namespace scidb
{

class ParsingContext;

/*
 * Base exception class
 */
class
__attribute__((visibility("default")))
Exception: public virtual std::exception
{
public:
    typedef std::shared_ptr<Exception> Pointer;

    Exception(){}

    Exception(const char* file, const char* function, int32_t line,
              const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
              const char* stringified_short_error_code, const char* stringified_long_error_code,
              const QueryID& query_id = INVALID_QUERY_ID);

    virtual ~Exception() throw ()
    {}

    const std::string& getErrorsNamespace() const;

    const char* what() const throw();

    const std::string& getFile() const;

    const std::string& getFunction() const;

    int32_t getLine() const;

    const std::string& getWhatStr() const;

    int32_t getShortErrorCode() const;

    int32_t getLongErrorCode() const;

    const std::string& getStringifiedShortErrorCode() const;

    const std::string& getStringifiedLongErrorCode() const;

    virtual Exception::Pointer copy() const = 0;

    virtual void raise() const = 0;

    const std::string getErrorId() const;

    const std::string getStringifiedErrorId() const;

    std::string getErrorMessage() const;

    const QueryID& getQueryId() const;

    void setQueryId(const QueryID& queryId);

    void setWhatStr(const std::string& whatStr);
protected:
    virtual void format() = 0;
    boost::format& getMessageFormatter() const;

    std::string _file;
    std::string _function;
    int32_t _line;
    std::string _errors_namespace;
    int32_t _short_error_code;
    int32_t _long_error_code;
    std::string _stringified_short_error_code;
    std::string _stringified_long_error_code;
    QueryID _query_id;

    std::string _what_str;
    mutable std::string _formatted_msg;
    mutable boost::format _formatter;
};

/*
 * Exceptions caused by some user-actions
 */
class
__attribute__((visibility("default")))
UserException: public Exception
{
public:
    typedef std::shared_ptr<UserException> Pointer;

    UserException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const QueryID& query_id = INVALID_QUERY_ID);

    UserException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* err_str,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const QueryID& query_id);

	~UserException() throw () {}

    template <class T>
    UserException& operator <<(const T &param)
    {
        try
        {
            getMessageFormatter() % param;
        }
        catch (std::exception& e)
        {
            if (isDebug()) {
                const std::string what = e.what();
                // Generate a core to analyze 'what'
                assert(false);
            }
        }
        catch (...)
        {
            // Silently ignore errors during adding parameters,
            // but not in debug builds.
            assert(false);
        }

        return *this;
    }

    Exception::Pointer copy() const;

    void raise() const;
private:
    void format();

    template <class T> friend std::shared_ptr<Exception> operator <<(std::shared_ptr<Exception> e, const T &param);
};

template <class T>
std::shared_ptr<UserException> operator <<(std::shared_ptr<UserException> e, const T &param)
{
    (*e) << param;
    return e;
}

/*
 * Exceptions caused by wrong queries
 */
class
__attribute__((visibility("default")))
UserQueryException: public UserException
{
public:
    typedef std::shared_ptr<UserQueryException> Pointer;

    UserQueryException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const std::shared_ptr<ParsingContext>& parsingContext, const QueryID& query_id = INVALID_QUERY_ID);

    UserQueryException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* err_str,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const std::shared_ptr<ParsingContext>& parsingContext, const QueryID& query_id);

	~UserQueryException() throw () {}

    std::shared_ptr<ParsingContext> getParsingContext() const;

    template <class T>
    UserQueryException& operator <<(const T &param)
    {
        try
        {
            getMessageFormatter() % param;
        }
        catch (std::exception& e)
        {
            if (isDebug()) {
                // Generate a core to analyze 'what'
                const std::string what = e.what();
                assert(false);
            }
        }
        catch (...)
        {
            // Silently ignore errors during adding parameters,
            // but not in debug builds.
            assert(false);
        }

        return *this;
    }

    Exception::Pointer copy() const;

    void raise() const;
private:
    std::shared_ptr<ParsingContext> _parsingContext;

    void format();
};

template <class T>
std::shared_ptr<UserQueryException> operator <<(std::shared_ptr<UserQueryException> e, const T &param)
{
    (*e) << param;
    return e;
}

/*
 * Exceptions caused by some errors in core
 */
class
__attribute__((visibility("default")))
SystemException: public Exception
{
public:
    typedef std::shared_ptr<SystemException> Pointer;

    SystemException(const char* file, const char* function, int32_t line,
                    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
                    const char* stringified_short_error_code, const char* stringified_long_error_code,
                    const QueryID& query_id = INVALID_QUERY_ID);

    SystemException(const char* file, const char* function, int32_t line,
                    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
                    const char* err_str,
                    const char* stringified_short_error_code, const char* stringified_long_error_code,
                    const QueryID& query_id);

    template <class T>
    SystemException& operator <<(const T &param)
    {
        try
        {
            getMessageFormatter() % param;
        }
        catch (std::exception& e)
        {
            if (isDebug()) {
                const std::string what = e.what();
                // Generate a core to analyze 'what'
                assert(false);
            }
        }
        catch (...)
        {
            // Silently ignore errors during adding parameters,
            // but not in debug builds
            assert(false);
        }

        return *this;
    }

	Exception::Pointer copy() const;

    void raise() const;

    ~SystemException() throw () {}
private:
    void format();
};

template <class T>
std::shared_ptr<SystemException> operator <<(std::shared_ptr<SystemException> e, const T &param)
{
    (*e) << param;
    return e;
}

} // namespace

#endif /* EXCEPTIONS_H_ */

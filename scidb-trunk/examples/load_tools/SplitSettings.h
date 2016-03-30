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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef SPLIT_SETTINGS
#define SPLIT_SETTINGS

namespace scidb
{

class SplitSettings
{
private:
    std::string  _inputFilePath;
    int64_t _linesPerChunk;
    bool    _linesPerChunkSet;
    int64_t _bufferSize;
    bool    _bufferSizeSet;
    int64_t _sourceInstanceId;
    bool    _sourceInstanceIdSet;
    char    _delimiter;
    bool    _delimiterSet;
    int64_t _header;
    bool    _headerSet;

public:
    static const size_t MAX_PARAMETERS = 6;

    SplitSettings(std::vector<std::shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 std::shared_ptr<Query>& query):
       _inputFilePath(""),
       _linesPerChunk(1000000),
       _linesPerChunkSet(false),
       _bufferSize(10*1024*1024),
       _bufferSizeSet(false),
       _sourceInstanceId(0),
       _sourceInstanceIdSet(false),
       _delimiter('\n'),
       _delimiterSet(false),
       _header(0),
       _headerSet(false)
    {
        std::string const inputFilePathHeader        = "input_file_path=";
        std::string const linesPerChunkHeader        = "lines_per_chunk=";
        std::string const bufferSizeHeader           = "buffer_size=";
        std::string const sourceInstanceIdHeader     = "source_instance_id=";
        std::string const delimiterHeader            = "delimiter=";
        std::string const headerHeader               = "header=";
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to SplitSettings";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            std::shared_ptr<OperatorParam>const& param = operatorParameters[i];
            std::string parameterString;
            if (logical)
            {
                parameterString = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((std::shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if      (boost::algorithm::starts_with(parameterString, inputFilePathHeader))
            {
                if (_inputFilePath != "")
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input file path multiple times";
                }
                std::string paramContent = parameterString.substr(inputFilePathHeader.size());
                boost::algorithm::trim(paramContent);
                _inputFilePath = paramContent;
            }
            else if (boost::algorithm::starts_with(parameterString, headerHeader))
            {
                if (_headerSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the header multiple times";
                }
                std::string paramContent = parameterString.substr(headerHeader.size());
                boost::algorithm::trim(paramContent);
                _header = boost::lexical_cast<int64_t>(paramContent);
                _headerSet = true;
            }
            else if (boost::algorithm::starts_with(parameterString, linesPerChunkHeader))  //yeah, yeah it's a long function...
            {
                if (_linesPerChunkSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set lines_per_chunk multiple times";
                }
                std::string paramContent = parameterString.substr(linesPerChunkHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                    _linesPerChunk = boost::lexical_cast<int64_t>(paramContent);
                    if(_linesPerChunk<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "lines_per_chunk must be positive";
                    }
                    _linesPerChunkSet = true;
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse lines_per_chunk";
                }
            }
            else if (boost::algorithm::starts_with(parameterString, bufferSizeHeader))
            {
                if (_bufferSizeSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set buffer_size multiple times";
                }
                std::string paramContent = parameterString.substr(bufferSizeHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                    _bufferSize = boost::lexical_cast<int64_t>(paramContent);
                    if(_bufferSize<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "lines_per_chunk must be positive";
                    }
                    _bufferSizeSet = true;
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse buffer_size";
                }
            }
            else if (boost::algorithm::starts_with (parameterString, sourceInstanceIdHeader))
            {
                if(_sourceInstanceIdSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set source_instance_id multiple times";
                }
                std::string paramContent = parameterString.substr(sourceInstanceIdHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                    _sourceInstanceId = boost::lexical_cast<int64_t>(paramContent);
                    if(_sourceInstanceId != 0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "source_instance_id may only be 0 (for now)";
                    }
                    _sourceInstanceIdSet = true;
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse source_instance_id";
                }
            }
            else if (boost::algorithm::starts_with(parameterString, delimiterHeader))
            {
                if(_delimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set delimiter multiple times";
                }
                std::string paramContent = parameterString.substr(delimiterHeader.size());
                boost::algorithm::trim(paramContent);
                if (paramContent == "\\t")
                {
                    _delimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _delimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _delimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _delimiter = ' ';
                }
                else
                {
                    try
                    {
                       _delimiter = boost::lexical_cast<char>(paramContent);
                    }
                    catch (boost::bad_lexical_cast const& exn)
                    {
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse delimiter";
                    }
                }
                _delimiterSet = true;
            }
            else
            {
                if (_inputFilePath != "")
                {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input file path multiple times";
                }
                std::string path = parameterString;
                boost::algorithm::trim(path);
                _inputFilePath = path;
            }
        }
        if (_inputFilePath == "")
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "input file path was not provided";
        }
    }

    std::string const& getInputFilePath() const
    {
        return _inputFilePath;
    }

    size_t getLinesPerChunk() const
    {
        return _linesPerChunk;
    }

    size_t getBufferSize() const
    {
        return _bufferSize;
    }

    int64_t getSourceInstanceId() const
    {
        return _sourceInstanceId;
    }

    char getDelimiter() const
    {
        return _delimiter;
    }

    int64_t getHeader() const
    {
        return _header;
    }
};

}

#endif //SPLIT_SETTINGS

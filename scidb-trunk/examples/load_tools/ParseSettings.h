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

#ifndef PARSE_SETTINGS
#define PARSE_SETTINGS

namespace scidb
{

class ParseSettings
{
private:
    int64_t _numAttributes;
    int64_t _chunkSize;
    bool    _chunkSizeSet;
    char    _attributeDelimiter;
    bool    _attributeDelimiterSet;
    char    _lineDelimiter;
    bool    _lineDelimiterSet;
    bool    _splitOnDimension;
    bool    _splitOnDimensionSet;

public:
    static const size_t MAX_PARAMETERS = 5;

    ParseSettings(std::vector<std::shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 std::shared_ptr<Query>& query):
       _numAttributes(0),
       _chunkSize(1000000),
       _chunkSizeSet(false),
       _attributeDelimiter('\t'),
       _attributeDelimiterSet(false),
       _lineDelimiter('\n'),
       _lineDelimiterSet(false),
       _splitOnDimension(false),
       _splitOnDimensionSet(false)
    {
        std::string const numAttributesHeader        = "num_attributes=";
        std::string const chunkSizeHeader            = "chunk_size=";
        std::string const attributeDelimiterHeader   = "attribute_delimiter=";
        std::string const lineDelimiterHeader        = "line_delimiter=";
        std::string const splitOnDimensionHeader     = "split_on_dimension=";
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
            if      (boost::algorithm::starts_with(parameterString, numAttributesHeader))
            {
                if (_numAttributes != 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input file path multiple times";
                }
                std::string paramContent = parameterString.substr(numAttributesHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                    _numAttributes = boost::lexical_cast<int64_t>(paramContent);
                    if(_numAttributes<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "num_attributes must be positive";
                    }
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse num_attributes";
                }
            }
            else if (boost::algorithm::starts_with(parameterString, chunkSizeHeader))
            {
                if (_chunkSizeSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set chunk_size multiple times";
                }
                std::string paramContent = parameterString.substr(chunkSizeHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                    _chunkSize = boost::lexical_cast<int64_t>(paramContent);
                    if(_chunkSize<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk_size must be positive";
                    }
                    _chunkSizeSet = true;
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk_size";
                }
            }
            else if (boost::algorithm::starts_with(parameterString, attributeDelimiterHeader))
            {
                if (_attributeDelimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set attribute_delimiter multiple times";
                }
                std::string paramContent = parameterString.substr(attributeDelimiterHeader.size());
                boost::algorithm::trim(paramContent);
                if (paramContent == "\\t")
                {
                    _attributeDelimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _attributeDelimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _attributeDelimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _attributeDelimiter = ' ';
                }
                else
                {
                    try
                    {
                        _attributeDelimiter = boost::lexical_cast<char>(paramContent);
                    }
                    catch (boost::bad_lexical_cast const& exn)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse attribute_delimiter";
                    }
                }
                _attributeDelimiterSet = true;
            }
            else if (boost::algorithm::starts_with (parameterString, lineDelimiterHeader))
            {
                if(_lineDelimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set line_delimiter multiple times";
                }
                std::string paramContent = parameterString.substr(lineDelimiterHeader.size());
                boost::algorithm::trim(paramContent);
                if (paramContent == "\\t")
                {
                    _lineDelimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _lineDelimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _lineDelimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _lineDelimiter = ' ';
                }
                else
                {
                    try
                    {
                        _lineDelimiter = boost::lexical_cast<char>(paramContent);
                    }
                    catch (boost::bad_lexical_cast const& exn)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse line_delimiter";
                    }
                }
                _lineDelimiterSet = true;
            }
            else if (boost::algorithm::starts_with (parameterString, splitOnDimensionHeader))
            {
                if(_splitOnDimensionSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set split_on_dimension multiple times";
                }
                std::string paramContent = parameterString.substr(splitOnDimensionHeader.size());
                boost::algorithm::trim(paramContent);
                try
                {
                   _splitOnDimension = boost::lexical_cast<bool>(paramContent);
                }
                catch (boost::bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse split_on_dimension";
                }
            }
            else
            {
                std::ostringstream err;
                err<<"Unrecognized parameter: "<<parameterString;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
            }
        }
        if (_numAttributes == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "num_attributes was not provided";
        }
    }

    size_t getNumAttributes() const
    {
        return _numAttributes;
    }

    size_t getChunkSize() const
    {
        return _chunkSize;
    }

    char getAttributeDelimiter() const
    {
        return _attributeDelimiter;
    }

    char getLineDelimiter() const
    {
        return _lineDelimiter;
    }

    bool getSplitOnDimension() const
    {
        return _splitOnDimension;
    }
};

}

#endif //PARSE_SETTINGS

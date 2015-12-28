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
 * To list(view) revisions r7590 and prior:
 *
 *    $ svn log(cat) https://svn.scidb.net/scidb/trunk/src/smgr/io/DBLoader.cpp@7590
 *
 * (I neglected to use 'svn copy ...' when renaming this file, sorry!  -mjl)
 */

#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#  define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#  define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#include "util/FileIO.h"

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <float.h>
#include <string>
#include <errno.h>

#include <boost/archive/text_oarchive.hpp>
#include <boost/format.hpp>
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <system/Exceptions.h>
#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <smgr/io/ArrayWriter.h>
#include <array/DBArray.h>
#include <smgr/io/Storage.h>
#include <system/SystemCatalog.h>
#include <smgr/io/TemplateParser.h>

using namespace std;
using namespace boost;
using namespace boost::archive;

namespace scidb
{
    struct AwIoError {
        AwIoError(int x) : error(x) {}
        int     error;
    };

#   define Fputc(_c, _fp)                       \
    do {                                        \
        int e = ::fputc(_c, _fp);               \
        if (e == EOF) {                         \
            e = errno ? errno : EIO;            \
            throw AwIoError(e);                 \
        }                                       \
    } while (0)

#   define Fputs(_s, _fp)                       \
    do {                                        \
        int e = ::fputs(_s, _fp);               \
        if (e == EOF) {                         \
            e = errno ? errno : EIO;            \
            throw AwIoError(e);                 \
        }                                       \
    } while (0)

#   define Fprintf(_fp,  _vargs...)             \
    do {                                        \
        int e = ::fprintf(_fp, _vargs);         \
        if (e < 0) {                            \
            e = errno ? errno : EIO;            \
            throw AwIoError(e);                 \
        }                                       \
    } while (0)

    int ArrayWriter::_precision = ArrayWriter::DEFAULT_PRECISION;

    static const char* supportedFormats[] = {
        "csv", "dense", "csv+", "lcsv+", "text", "sparse", "lsparse",
        "store", "opaque", "dcsv", "tsv", "tsv+", "ltsv+"
    };
    static const unsigned NUM_FORMATS = SCIDB_SIZE(supportedFormats);

    // declared static to prevent visibility of variable outside of this file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.io.ArrayWriter"));

    /*
     * XXX This seems to have something to do with the EmptyTag
     * attribute for l<xxx> formats, but it would be great if someone
     * more familiar could elaborate!
     */
    class CompatibilityIterator : public ConstChunkIterator
    {
        Coordinates currPos;
        std::shared_ptr<ConstChunkIterator> inputIterator;
        Coordinates const& firstPos;
        Coordinates const& lastPos;
        Coordinates const* nextPos;
        bool hasCurrent;
        Value defaultValue;
        int mode;
        bool isEmptyable;

      public:
        CompatibilityIterator(std::shared_ptr<ConstChunkIterator> iterator, bool isSparse)
        : inputIterator(iterator),
          firstPos(iterator->getFirstPosition()),
          lastPos(iterator->getLastPosition()),
          defaultValue(iterator->getChunk().getAttributeDesc().getDefaultValue()),
          mode(iterator->getMode()),
          isEmptyable(iterator->getChunk().getArrayDesc().getEmptyBitmapAttribute() != NULL)
        {
            if (isSparse) {
                mode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
            }
            mode &= ~ConstChunkIterator::IGNORE_DEFAULT_VALUES;
            reset();
        }

        bool skipDefaultValue() {
            return false;
        }

        int getMode() const {
            return inputIterator->getMode();
        }

        Value const& getItem() {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return (nextPos == NULL || currPos != *nextPos) ? defaultValue : inputIterator->getItem();
        }

        bool isEmpty() const {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return isEmptyable && (nextPos == NULL || currPos != *nextPos);
        }

        bool end() {
            return !hasCurrent;
        }

        void operator ++() {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);

            do {
                if (mode & ConstChunkIterator::IGNORE_EMPTY_CELLS) {
                    ++(*inputIterator);
                    if (inputIterator->end()) {
                        hasCurrent = false;
                        return;
                    }
                    nextPos = &inputIterator->getPosition();
                    currPos = *nextPos;
                } else {
                    if (nextPos != NULL && currPos == *nextPos) {
                        ++(*inputIterator);
                        nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
                    }
                    size_t i = currPos.size()-1;
                    while (++currPos[i] > lastPos[i]) {
                        if (i == 0) {
                            hasCurrent = false;
                            return;
                        }
                        currPos[i] = firstPos[i];
                        i -= 1;
                    }
                }
            } while (skipDefaultValue());
        }

        Coordinates const& getPosition() {
            return currPos;
        }

        bool setPosition(Coordinates const& pos) {
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NOT_IMPLEMENTED)
                << "CompatibilityIterator::setPosition";
        }

        void reset() {
            inputIterator->reset();
            nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
            hasCurrent = nextPos != NULL || !(mode & ConstChunkIterator::IGNORE_EMPTY_CELLS);
            currPos = (mode & ConstChunkIterator::IGNORE_EMPTY_CELLS) && nextPos ? *nextPos : firstPos;
            if (hasCurrent && skipDefaultValue()) {
                ++(*this);
            }
        }

        ConstChunk const& getChunk() {
            return inputIterator->getChunk();
        }
    };

    static void checkStreamError(FILE *f, char const* where)
    {
        int rc = ferror(f);
        if (rc)
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << "Error flag set on FILE handle" << where;
    }

    /**
     * Parameters and utility methods for "foo-separated values" formats.
     */
    class XsvParms {
    public:

        /** For historical reasons, use single-quote for CSV quoted strings. */
        static const char DEFAULT_CSV_QUOTE = '\'';

        /**
         * The default XsvParms object corresponds to 'csv'.
         *
         * The options string is derived from the SAVE operator's format parameter, whose syntax is
         *
         *     BASE_FORMAT [ : [ OPTIONS ] ]
         *
         * The XsvParms object provides different behaviors for printing of nulls depending on
         * the presence of various single characters in the options string.  The comments in the
         * switch statement describe the current possibilities.
         *
         * @param options string containing single character option designators
         *
         * @see wiki:Development/components/CsvTsvFormatOptions
         */
        XsvParms(const string& format)
            : _delim(',')
            , _pretty(false)
            , _wantCoords(false)
            , _compatMode(false)
            , _parallel(false)
            , _wantLabels(false)
            , _formatter(format)
        {
            string::size_type colon = format.find(':');
            if (colon != string::npos) {
                _wantLabels =
                    (format.substr(colon + 1).find('l') != string::npos);
            }
        }

        XsvParms& setDelim(char ch) {
            _delim = ch;
            return *this;
        }
        XsvParms& setPretty(bool b) {
            _pretty = b;
            return *this;
        }
        XsvParms& setCoords(bool b) {
            _wantCoords = b;
            return *this;
        }
        XsvParms& setCompat(bool b) {
            _compatMode = b;
            return *this;
        }
        XsvParms& setParallel(bool b) {
            _parallel = b;
            return *this;
        }
        XsvParms& setLabels(bool b) {
            _wantLabels = b;
            return *this;
        }
        XsvParms& setPrecision(int p) {
            (void) _formatter.setPrecision(p);
            return *this;
        }

        char delim() const { return _delim; }
        bool pretty() const { return _pretty; }
        bool wantCoords() const { return _wantCoords; }
        bool compatMode() const { return _compatMode; }
        bool parallelSave() const { return _parallel; }
        bool wantLabels() const { return _wantLabels; }
        Value::Formatter const& getFormatter() const { return _formatter; }

        /**
         * Encoding for TSV string fields.
         * @see http://dataprotocols.org/linear-tsv/
         */
        string encodeString(const char *s) const;

    private:
        char _delim;
        bool _pretty;
        bool _wantCoords;
        bool _compatMode;
        bool _parallel;
        bool _wantLabels;
        Value::Formatter _formatter;
    };

    const char XsvParms::DEFAULT_CSV_QUOTE;

    static void s_fprintValue(FILE *f,
                              const Value* v,
                              TypeId const& valueType,
                              FunctionPointer const converter,
                              Value::Formatter const& vf)
    {
        static const TypeId STRING_TYPE_ID(TID_STRING);

        Value strValue;
        TypeId const* tidp = &valueType;
        if (converter) {
            (*converter)(&v, &strValue, NULL);
            // Pretend we were working on the strValue string all along!
            v = &strValue;
            tidp = &STRING_TYPE_ID;
        }

        Fprintf(f, "%s", v->toString(*tidp, vf).c_str());
    }

    static void s_fprintCoordinate(FILE *f,
                                   Coordinate ordinalCoord)
    {
        Fprintf(f, "%" PRIi64, ordinalCoord);
    }

    static void s_fprintCoordinates(FILE *f,
                                    Coordinates const& coords)
    {
        Fputc('{', f);
        for (size_t i = 0; i < coords.size(); i++)
        {
            if (i != 0)
            {
                Fputc(',', f);
            }
            s_fprintCoordinate(f, coords[i]);
        }
        Fputc('}', f);
    }

    static void printLabels(FILE* f,
                            Dimensions const& dims,
                            Attributes const& attrs,
                            AttributeDesc const* emptyAttr,
                            XsvParms const& parms)
    {
        // Dimensions first.
        if (parms.wantCoords()) {
            if (parms.pretty())
                Fputc('{', f);
            for (unsigned i = 0; i < dims.size(); ++i) {
                if (i) {
                    Fputc(parms.delim(), f);
                }
                Fprintf(f, "%s", dims[i].getBaseName().c_str());
            }
            if (parms.pretty()) {
                Fputs("} ", f);
            } else {
                Fputc(parms.delim(), f);
            }
        }

        // Then attributes.
        for (unsigned i = 0, j = 0; i < attrs.size(); ++i) {
            if (emptyAttr && emptyAttr == &attrs[i])
                continue; // j not incremented!
            if (j++) {
                Fputc(parms.delim(), f);
            }
            Fprintf(f, "%s", attrs[i].getName().c_str());
        }
        Fputc('\n', f);
    }

    /**
     * @brief Single code path for "foo-separated values" formats.
     *
     * @description The handling of some text formats is remarkably
     * similar, and can be parameterized via an @c XsvParms object.
     * Currently supported formats are csv, csv+, lcsv+, tsv, tsv+,
     * ltsv+ and dcsv.
     */
    static uint64_t saveXsvFormat(Array const& array,
                                  ArrayDesc const& desc,
                                  FILE* f,
                                  const XsvParms& parms)
    {
        // No attributes, no problem.
        Attributes const& attrs = desc.getAttributes();
        AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
        unsigned numAttrs = attrs.size() - (emptyAttr ? 1 : 0);
        if (numAttrs == 0) {
            checkStreamError(f, "No attributes");
            return 0;
        }

        // Gather various per-attribute items.
        vector<std::shared_ptr<ConstArrayIterator> > arrayIterators(numAttrs);
        vector<FunctionPointer>                 converters(numAttrs);
        vector<TypeId>                          types(numAttrs);
        for (unsigned i = 0, j = 0; i < attrs.size(); ++i) {
            if (emptyAttr && emptyAttr == &attrs[i])
                continue; // j not incremented!
            arrayIterators[j] = array.getConstIterator(i);
            types[j] = attrs[i].getType();
            if (!isBuiltinType(types[j])) {
                converters[j] = FunctionLibrary::getInstance()->findConverter(
                    types[j],
                    TID_STRING,
                    false);
            }
            ++j;
        }

        // Labels are trouble for parallel saves (i.e. on subsequent reload).
        if (!parms.parallelSave() && parms.wantLabels()) {
            printLabels(f, desc.getDimensions(), attrs, emptyAttr, parms);
        }

        // Time to walk the chunks!
        uint64_t count = 0;
        vector<std::shared_ptr<ConstChunkIterator> > chunkIterators(numAttrs);
        const int CHUNK_MODE =
            ConstChunkIterator::IGNORE_OVERLAPS |
            ConstChunkIterator::IGNORE_EMPTY_CELLS;
        while (!arrayIterators[0]->end()) {

            // Set up chunk iterators, one per attribute.
            for (unsigned i = 0; i < numAttrs; ++i) {
                ConstChunk const& chunk = arrayIterators[i]->getChunk();
                chunkIterators[i] = chunk.getConstIterator(CHUNK_MODE);
                if (parms.compatMode()) {
                    // This compatibility wrapper must do something cool.
                    chunkIterators[i] = std::shared_ptr<ConstChunkIterator>(
                        new CompatibilityIterator(chunkIterators[i],
                                                  false));
                }
            }

            // Print these chunks...
            while (!chunkIterators[0]->end()) {

                // Coordinates, anyone?
                if (parms.wantCoords()) {
                    Coordinates const& pos = chunkIterators[0]->getPosition();
                    if (parms.pretty())
                        Fputc('{', f);
                    for (unsigned i = 0; i < pos.size(); ++i) {
                        if (i) {
                            Fputc(parms.delim(), f);
                        }
                        Fprintf(f, "%" PRIi64, pos[i]);
                    }
                    if (parms.pretty()) {
                        Fputs("} ", f);
                    } else {
                        Fputc(parms.delim(), f);
                    }
                }

                // Then come the attributes.  Bump their corresponding
                // chunk iterators as we go.
                for (unsigned i = 0; i < numAttrs; ++i) {
                    if (i) {
                        Fputc(parms.delim(), f);
                    }
                    s_fprintValue(f,
                                  &chunkIterators[i]->getItem(),
                                  types[i],
                                  converters[i],
                                  parms.getFormatter());
                    ++(*chunkIterators[i]);
                }

                // Another array cell for peace!
                count += 1;
                Fputc('\n', f);
            }

            // Bump the array iterators to get the next set of chunks.
            for (unsigned i = 0; i < numAttrs; ++i) {
                ++(*arrayIterators[i]);
            }
        }

        checkStreamError(f, __FUNCTION__);
        return count;
    }

    /**
     * This code handles sparse, dense, store, and text formats.
     */
    static uint64_t saveTextFormat(Array const& array,
                                   ArrayDesc const& desc,
                                   FILE* f,
                                   std::string const& format)
    {
        size_t i, j;
        uint64_t n = 0;
        int precision = ArrayWriter::getPrecision();
        Attributes const& attrs = desc.getAttributes();
        //If descriptor has empty flag we just ignore it and fill only iterators with actual data attributes
        bool omitEmptyTag = desc.getEmptyBitmapAttribute();
        size_t iteratorsCount = attrs.size() - (omitEmptyTag ? 1 : 0);
        if (iteratorsCount == 0)
        {
            checkStreamError(f, __FUNCTION__);
            return n;
        }

        Dimensions const& dims = desc.getDimensions();
        const size_t nDimensions = dims.size();
        assert(nDimensions > 0);
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(iteratorsCount);
        vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(iteratorsCount);
        vector< TypeId> types(iteratorsCount);
        vector< FunctionPointer> converters(iteratorsCount);
        Coordinates coord(nDimensions);
        int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;

        // Get array iterators for all attributes
        for (i = 0, j = 0; i < attrs.size(); i++)
        {
            if (omitEmptyTag && attrs[i] == *desc.getEmptyBitmapAttribute())
                continue;

            arrayIterators[j] = array.getConstIterator((AttributeID)i);
            types[j] = attrs[i].getType();
            if (! isBuiltinType(types[j])) {
                converters[j] =  FunctionLibrary::getInstance()->findConverter(types[j], TID_STRING, false);
            }
            ++j;
        }

        bool sparseFormat = compareStringsIgnoreCase(format, "sparse") == 0;
        bool denseFormat = compareStringsIgnoreCase(format, "dense") == 0;
        bool storeFormat = compareStringsIgnoreCase(format, "store") == 0;
        bool autoFormat = compareStringsIgnoreCase(format, "text") == 0;

        bool startOfArray = true;
        if (sparseFormat) {
            iterationMode |= ConstChunkIterator::IGNORE_EMPTY_CELLS;
        }
        if (storeFormat) {
            // We want what's really stored: *all* the floating point digits, overlap regions.
            if (precision < DBL_DIG) {
                precision = DBL_DIG;
            }
            iterationMode &= ~ConstChunkIterator::IGNORE_OVERLAPS;
        }

        Value::Formatter textFormatter;
        textFormatter.setPrecision(precision);

        // Set initial position
        Coordinates chunkPos(nDimensions);
        for (i = 0; i < nDimensions; i++) {
            coord[i] = dims[i].getStartMin();
            chunkPos[i] = dims[i].getStartMin();
        }

        // Check if chunking is performed in more than one dimension
        bool multisplit = false;
        for (i = 1; i < nDimensions; i++) {
            if (dims[i].getChunkInterval() < static_cast<int64_t>(dims[i].getLength())) {
                multisplit = true;
            }
        }

        coord[nDimensions-1] -= 1; // to simplify increment
        chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
        {
            // Iterate over all chunks
            bool firstItem = true;
            while (!arrayIterators[0]->end()) {
                // Get iterators for the current chunk
                bool isSparse = false;
                for (i = 0; i < iteratorsCount; i++) {
                    ConstChunk const& chunk = arrayIterators[i]->getChunk();
                    chunkIterators[i] = chunk.getConstIterator(iterationMode);
                    if (i == 0) {
                        isSparse = !denseFormat &&
                            (autoFormat && chunk.count()*100/chunk.getNumberOfElements(false) <= 10);
                    }
                    chunkIterators[i] = std::shared_ptr<ConstChunkIterator>(
                         new CompatibilityIterator(chunkIterators[i], isSparse));
                }
                int j = nDimensions;
                while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax()) {
                    chunkPos[j] = dims[j].getStartMin();
                }
                bool gap = !storeFormat && (sparseFormat || arrayIterators[0]->getPosition() != chunkPos);
                chunkPos = arrayIterators[0]->getPosition();
                if (!sparseFormat || !chunkIterators[0]->end()) {
                    if (!multisplit) {
                        Coordinates const& last = chunkIterators[0]->getLastPosition();
                        for (i = 1; i < nDimensions; i++) {
                            if (last[i] < dims[i].getEndMax()) {
                                multisplit = true;
                            }
                        }
                    }
                    if (isSparse || storeFormat) {
                        if (!firstItem) {
                            firstItem = true;
                            for (i = 0; i < nDimensions; i++) {
                                Fputc(']', f);
                            }
                            Fprintf(f, ";\n");
                            if (storeFormat) {
                                Fputc('{', f);
                                for (i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, chunkPos[i]);
                                }
                                Fputc('}', f);
                            }
                            for (i = 0; i < nDimensions; i++) {
                                Fputc('[', f);
                            }
                        }
                    }
                    if (storeFormat) {
                        coord =  chunkIterators[0]->getChunk().getFirstPosition(true);
                        coord[nDimensions-1] -= 1; // to simplify increment
                    }
                    // Iterator over all chunk elements
                    while (!chunkIterators[0]->end()) {
                        if (!isSparse) {
                            Coordinates const& pos = chunkIterators[0]->getPosition();
                            int nbr = 0;
                            for (i = nDimensions-1; pos[i] != ++coord[i]; i--) {
                                if (!firstItem) {
                                    Fputc(']', f);
                                    nbr += 1;
                                }
                                if (multisplit) {
                                    coord[i] = pos[i];
                                    if (i == 0) {
                                        break;
                                    }
                                } else {
                                    if (i == 0) {
                                        break;
                                    } else {
                                        coord[i] = dims[i].getStartMin();
                                        if (sparseFormat) {
                                            coord[i] = pos[i];
                                            if (i == 0) {
                                                break;
                                            }
                                        } else {
                                            assert(coord[i] == pos[i]);
                                            assert(i != 0);
                                        }
                                    }
                                }
                            }
                            if (!firstItem) {
                                Fputc(nbr == (int)nDimensions ? ';' : ',', f);
                            }
                            if (gap) {
                                Fputc('{', f);
                                for (i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, pos[i]);
                                    coord[i] = pos[i];
                                }
                                Fputc('}', f);
                                gap = false;
                            }
                            if (startOfArray) {
                                if (storeFormat) {
                                    Fputc('{', f);
                                    for (i = 0; i < nDimensions; i++) {
                                        if (i != 0) {
                                            Fputc(',', f);
                                        }
                                        Fprintf(f, "%" PRIi64, chunkPos[i]);
                                    }
                                    Fputc('}', f);
                                }
                                for (i = 0; i < nDimensions; i++) {
                                    Fputc('[', f);
                                }
                                startOfArray = false;
                            }
                            while (--nbr >= 0) {
                                Fputc('[', f);
                            }
                            if (sparseFormat) {
                                Fputc('{', f);
                                Coordinates const& pos = chunkIterators[0]->getPosition();
                                for (i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, pos[i]);
                                }
                                Fputc('}', f);
                            }
                        } else {
                            if (!firstItem) {
                                Fputc(',', f);
                            }
                            if (startOfArray) {
                                if (storeFormat) {
                                    Fputc('{', f);
                                    for (i = 0; i < nDimensions; i++) {
                                        if (i != 0) {
                                            Fputc(',', f);
                                        }
                                        Fprintf(f, "%" PRIi64, chunkPos[i]);
                                    }
                                    Fputc('}', f);
                                }
                                for (i = 0; i < nDimensions; i++) {
                                    Fputc('[', f);
                                }
                                startOfArray = false;
                            }
                            Fputc('{', f);
                            Coordinates const& pos = chunkIterators[0]->getPosition();
                            for (i = 0; i < nDimensions; i++) {
                                if (i != 0) {
                                    Fputc(',', f);
                                }
                                Fprintf(f, "%" PRIi64, pos[i]);
                            }
                            Fputc('}', f);
                        }
                        Fputc('(', f);
                        if (!chunkIterators[0]->isEmpty()) {
                            for (i = 0; i < iteratorsCount; i++) {
                                if (i != 0) {
                                    Fputc(',', f);
                                }
                                const Value* v = &chunkIterators[i]->getItem();
                                s_fprintValue(f, v, types[i], converters[i], textFormatter);
                            }
                        }
                        n += 1;
                        firstItem = false;
                        Fputc(')', f);

                        for (i = 0; i < iteratorsCount; i++) {
                            ++(*chunkIterators[i]);
                        }
                    }
                }
                for (i = 0; i < iteratorsCount; i++) {
                    ++(*arrayIterators[i]);
                }
                if (multisplit) {
                    for (i = 0; i < nDimensions; i++) {
                        coord[i] = dims[i].getEndMax() + 1;
                    }
                }
            }
            if (startOfArray) {
                for (i = 0; i < nDimensions; i++) {
                    Fputc('[', f);
                }
                startOfArray = false;
            }
            for (i = 0; i < nDimensions; i++) {
                Fputc(']', f);
            }
        }
        Fputc('\n', f);

        checkStreamError(f, __FUNCTION__);
        return n;
    }


    /**
     * This code handles the lsparse format.
     */
    static uint64_t saveLsparseFormat(Array const& array,
                                      ArrayDesc const& desc,
                                      FILE* f,
                                      std::string const& format)
    {
        size_t i;
        uint64_t n = 0;
        Value::Formatter textFormatter;
        textFormatter.setPrecision(ArrayWriter::getPrecision());

        Attributes const& attrs = desc.getAttributes();
        size_t nAttributes = attrs.size();

        if (desc.getEmptyBitmapAttribute())
        {
            assert(desc.getEmptyBitmapAttribute()->getId() == desc.getAttributes().size()-1);
            nAttributes--;
        }

        if (nAttributes != 0)
        {
            Dimensions const& dims = desc.getDimensions();
            const size_t nDimensions = dims.size();
            assert(nDimensions > 0);
            vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(nAttributes);
            vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(nAttributes);
            vector< TypeId> attTypes(nAttributes);
            vector< FunctionPointer> attConverters(nAttributes);
            vector< FunctionPointer> dimConverters(nDimensions);
            vector<Value> origPos;

            int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS;

            for (i = 0; i < nAttributes; i++)
            {
                arrayIterators[i] = array.getConstIterator((AttributeID)i);
                attTypes[i] = attrs[i].getType();
            }

            Coordinates coord(nDimensions);
            bool startOfArray = true;

            // Set initial position
            Coordinates chunkPos(nDimensions);
            for (i = 0; i < nDimensions; i++)
            {
                coord[i] = dims[i].getStartMin();
                chunkPos[i] = dims[i].getStartMin();
            }

            // Check if chunking is performed in more than one dimension
            bool multisplit = false;
            for (i = 1; i < nDimensions; i++)
            {
                if (dims[i].getChunkInterval() < static_cast<int64_t>(dims[i].getLength()))
                {
                    multisplit = true;
                }
            }

            coord[nDimensions-1] -= 1; // to simplify increment
            chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
            {
                // Iterate over all chunks
                bool firstItem = true;
                while (!arrayIterators[0]->end())
                {
                    // Get iterators for the current chunk
                    for (i = 0; i < nAttributes; i++)
                    {
                        ConstChunk const& chunk = arrayIterators[i]->getChunk();
                        chunkIterators[i] = chunk.getConstIterator(iterationMode);
                    }

                    int j = nDimensions;
                    while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax())
                    {
                        chunkPos[j] = dims[j].getStartMin();
                    }
                    bool gap = true;
                    chunkPos = arrayIterators[0]->getPosition();
                    if ( !chunkIterators[0]->end())
                    {
                        if (!multisplit)
                        {
                            Coordinates const& last = chunkIterators[0]->getLastPosition();
                            for (i = 1; i < nDimensions; i++)
                            {
                                if (last[i] < dims[i].getEndMax())
                                {
                                    multisplit = true;
                                }
                            }
                        }

                        // Iterator over all chunk elements
                        while (!chunkIterators[0]->end())
                        {
                            {
                                Coordinates const& pos = chunkIterators[0]->getPosition();
                                int nbr = 0;
                                for (i = nDimensions-1; pos[i] != ++coord[i]; i--)
                                {
                                    if (!firstItem)
                                    {
                                        Fputc(']', f);
                                        nbr += 1;
                                    }
                                    if (multisplit)
                                    {
                                        coord[i] = pos[i];
                                        if (i == 0)
                                        {
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        if (i == 0)
                                        {
                                            break;
                                        }
                                        else
                                        {
                                            coord[i] = dims[i].getStartMin();
                                            coord[i] = pos[i];
                                            if (i == 0)
                                            {
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (!firstItem)
                                {
                                    Fputc(nbr == (int)nDimensions ? ';' : ',', f);
                                }
                                if (gap)
                                {
                                    s_fprintCoordinates(f, pos);
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        coord[i]=pos[i];
                                    }
                                    gap = false;
                                }
                                if (startOfArray)
                                {
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        Fputc('[', f);
                                    }
                                    startOfArray = false;
                                }
                                while (--nbr >= 0)
                                {
                                    Fputc('[', f);
                                }
                                s_fprintCoordinates(f, pos);
                            }
                            Fputc('(', f);
                            if (!chunkIterators[0]->isEmpty())
                            {
                                for (i = 0; i < nAttributes; i++)
                                {
                                    if (i != 0)
                                    {
                                        Fputc(',', f);
                                    }

                                    s_fprintValue(f, &chunkIterators[i]->getItem(),
                                                  attTypes[i], attConverters[i],
                                                  textFormatter);
                                }
                            }
                            n += 1;
                            firstItem = false;
                            Fputc(')', f);
                            for (i = 0; i < nAttributes; i++)
                            {
                                ++(*chunkIterators[i]);
                            }
                        }
                    }
                    for (i = 0; i < nAttributes; i++)
                    {
                        ++(*arrayIterators[i]);
                    }
                    if (multisplit)
                    {
                        for (i = 0; i < nDimensions; i++)
                        {
                            coord[i] = dims[i].getEndMax() + 1;
                        }
                    }
                }
                if (startOfArray)
                {
                    for (i = 0; i < nDimensions; i++)
                    {
                        Fputc('[', f);
                    }
                    startOfArray = false;
                }
                for (i = 0; i < nDimensions; i++)
                {
                    Fputc(']', f);
                }
            }
            Fputc('\n', f);
        }
        checkStreamError(f, __FUNCTION__);
        return n;
    }

#ifndef SCIDB_CLIENT
    static uint64_t saveOpaque(Array const& array,
                               ArrayDesc const& desc,
                               FILE* f,
                               std::shared_ptr<Query> const& query)
    {
        size_t nAttrs = desc.getAttributes().size();
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(nAttrs);
        uint64_t n;
        OpaqueChunkHeader hdr;
        setToZeroInDebug(&hdr, sizeof(hdr));

        hdr.version = SCIDB_OPAQUE_FORMAT_VERSION;
        hdr.signature = OpaqueChunkHeader::calculateSignature(desc);
        hdr.magic = OPAQUE_CHUNK_MAGIC;

        hdr.flags = OpaqueChunkHeader::ARRAY_METADATA;
        stringstream ss;
        text_oarchive oa(ss);
        oa & desc;
        string const& s = ss.str();
        hdr.size = s.size();
        if (fwrite(&hdr, sizeof(hdr), 1, f) != 1
            || fwrite(&s[0], 1, hdr.size, f) != hdr.size)
        {
            int err = errno ? errno : EIO;
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << ::strerror(err) << err;
        }

        for (size_t i = 0; i < nAttrs; i++) {
            arrayIterators[i] = array.getConstIterator(i);
        }
        for (n = 0; !arrayIterators[0]->end(); n++) {
            for (size_t i = 0; i < nAttrs; i++) {
                ConstChunk const* chunk = &arrayIterators[i]->getChunk();
                Coordinates const& pos = chunk->getFirstPosition(false);
                PinBuffer scope(*chunk);
                if (chunk->getSize() > (uint64_t)numeric_limits<uint32_t>::max())
                {
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                        << "Chunk larger than maximum size: " << chunk->getSize();
                }
                hdr.size = chunk->getSize();
                hdr.attrId = i;
                hdr.compressionMethod = chunk->getCompressionMethod();
                hdr.flags = 0;
                hdr.flags |= OpaqueChunkHeader::RLE_FORMAT;
                if (!chunk->getAttributeDesc().isEmptyIndicator()) {
                    // RLE chunks received from other nodes by SG contain empty bitmap.
                    // There is no need to save this bitmap in each chunk - so just cut it.
                    ConstRLEPayload payload((char*)chunk->getData());
                    assert(hdr.size >= payload.packedSize());
                    hdr.size = payload.packedSize();
                }
                hdr.nDims = pos.size();
                if (fwrite(&hdr, sizeof(hdr), 1, f) != 1
                    || fwrite(&pos[0], sizeof(Coordinate), hdr.nDims, f) != hdr.nDims
                    || fwrite(chunk->getData(), 1, hdr.size, f) != hdr.size)
                {
                    int err = errno ? errno : EIO;
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                        << ::strerror(err) << err;
                }
            }
            for (size_t i = 0; i < nAttrs; i++) {
                ++(*arrayIterators[i]);
            }
        }
        return n;
    }

    /// Compute bytes of padding to insert for 'skip' columns.
    static inline size_t skip_bytes(ExchangeTemplate::Column const& c)
    {
        SCIDB_ASSERT(c.skip);
        return (c.fixedSize ? c.fixedSize : sizeof(uint32_t)) + c.nullable;
    }

    /**
     * Write binary data based on a template.
     * @see scidb::BinaryChunkLoader::loadChunk
     */
    static uint64_t saveUsingTemplate(Array const& array,
                                      ArrayDesc const& desc,
                                      FILE* f,
                                      string const& format,
                                      std::shared_ptr<Query> const& query)
    {
        ExchangeTemplate templ = TemplateParser::parse(desc, format, false);
        const size_t N_ATTRS = desc.getAttributes(true /*exclude empty bitmap*/).size();
        const size_t N_COLUMNS = templ.columns.size();
        SCIDB_ASSERT(N_COLUMNS >= N_ATTRS); // One col per attr, plus "skip" columns
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(N_ATTRS);
        vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(N_ATTRS);
        vector< Value > cnvValues(N_ATTRS);
        vector< char > padBuffer(sizeof(uint64_t) + 1, '\0'); // Big enuf for all nullable built-ins
        size_t nMissingReasonOverflows = 0;

        for (size_t c = 0, i = 0; c < N_COLUMNS; ++c) {
            ExchangeTemplate::Column const& column = templ.columns[c];
            if (column.skip) {
                // Prepare to write (enough) padding.
                size_t pad = skip_bytes(column);
                if (pad > padBuffer.size()) {
                    padBuffer.resize(pad);
                }
            } else {
                // Prepare to write values.
                SCIDB_ASSERT(i < N_ATTRS);
                arrayIterators[i] = array.getConstIterator(i);
                if (column.converter) {
                    cnvValues[i] = Value(column.externalType);
                }
                ++i;            // next attribute
            }
        }

        uint64_t nCells = 0;    // aka number of tuples written
        for (size_t n = 0; !arrayIterators[0]->end(); n++) {
            for (size_t i = 0; i < N_ATTRS; i++) {
                chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(
                    ConstChunkIterator::IGNORE_OVERLAPS |
                    ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
            while (!chunkIterators[0]->end()) {
                ++nCells;
                for (size_t c = 0, i = 0; c < N_COLUMNS; ++c) {
                    ExchangeTemplate::Column const& column = templ.columns[c];
                    if (column.skip) {
                        // On output, skip means write NUL bytes (ticket #4703).
                        size_t pad = skip_bytes(column);
                        SCIDB_ASSERT(padBuffer.size() >= pad);
                        if (fwrite(&padBuffer[0], 1, pad, f) != pad) {
                            int err = errno ? errno : EIO;
                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                << ::strerror(err) << err;
                        }
                    }
                    else {
                        Value const* v = &chunkIterators[i]->getItem();
                        if (column.nullable) {
                            if (v->getMissingReason() > 127) {
                                LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason()
                                             << " cannot be stored in binary file");
                                nMissingReasonOverflows += 1;
                            }
                            int8_t missingReason = (int8_t)v->getMissingReason();
                            if (fwrite(&missingReason, sizeof(missingReason), 1, f) != 1) {
                                int err = errno ? errno : EIO;
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                    << ::strerror(err) << err;
                            }
                        }
                        if (v->isNull()) {
                            if (!column.nullable) {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                            }
                            // for varying size type write 4-bytes counter
                            size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
                            SCIDB_ASSERT(padBuffer.size() >= size);
                            if (fwrite(&padBuffer[0], 1, size, f) != size) {
                                int err = errno ? errno : EIO;
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                    << ::strerror(err) << err;
                            }
                        } else {
                            if (column.converter) {
                                column.converter(&v, &cnvValues[i], NULL);
                                v = &cnvValues[i];
                            }
                            if (v->size() > numeric_limits<uint32_t>::max()) {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION)
                                    << v->size() << numeric_limits<uint32_t>::max();
                            }
                            uint32_t size = (uint32_t)v->size();
                            if (column.fixedSize == 0) { // varying size type
                                if (fwrite(&size, sizeof(size), 1, f) != 1
                                    || fwrite(v->data(), 1, size, f) != size)
                                {
                                    int err = errno ? errno : EIO;
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                        << ::strerror(err) << err;
                                }
                            } else {
                                if (size > column.fixedSize) {
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION)
                                        << size << column.fixedSize;
                                }
                                if (fwrite(v->data(), 1, size, f) != size)
                                {
                                    int err = errno ? errno : EIO;
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                        << ::strerror(err) << err;
                                }
                                if (size < column.fixedSize) {
                                    size_t padSize = column.fixedSize - size;
                                    assert(padSize <= padBuffer.size());
                                    if (fwrite(&padBuffer[0], 1, padSize, f) != padSize)
                                    {
                                        int err = errno ? errno : EIO;
                                        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                            << ::strerror(err) << err;
                                    }
                                }
                            }
                        }
                        ++(*chunkIterators[i]);
                        ++i;    // next attribute
                    }
                }
            }
            for (size_t i = 0; i < N_ATTRS; i++) {
                ++(*arrayIterators[i]);
            }
        }
        if (nMissingReasonOverflows > 0) {
            query->postWarning(SCIDB_WARNING(SCIDB_LE_MISSING_REASON_OUT_OF_BOUNDS));
        }
        checkStreamError(f, __FUNCTION__);
        return nCells;
#       undef PAD
    }

#endif

    uint64_t ArrayWriter::save(Array const& array, string const& file,
                               const std::shared_ptr<Query>& query,
                               string const& format, unsigned flags)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        uint64_t n = 0;

        FILE* f;
        bool isBinary = compareStringsIgnoreCase(format, "opaque") == 0 || format[0] == '(';
        if (file == "console" || file == "stdout") {
            f = stdout;
        } else if (file == "stderr") {
            f = stderr;
        } else {
            bool append = flags & F_APPEND;
            f = fopen(file.c_str(), isBinary ? append ? "ab" : "wb" : append ? "a" : "w");
            if (NULL == f) {
                int error = errno;
                LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file
                              << "' failed: " << ::strerror(error) << " (" << error);
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_OPEN_FILE)
                    << file << ::strerror(error) << error;
            }
            struct flock flc;
            flc.l_type = F_WRLCK;
            flc.l_whence = SEEK_SET;
            flc.l_start = 0;
            flc.l_len = 1;

            int rc = fcntl(fileno(f), F_SETLK, &flc);
            if (rc == -1) {
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_LOCK_FILE)
                    << file << ::strerror(errno) << errno;
            }
        }

        // Switch out to "foo-separated values" if we can.
        std::shared_ptr<XsvParms> xParms;
        string::size_type colon = format.find(':');
        string baseFmt = format.substr(0, colon);
        if (compareStringsIgnoreCase(baseFmt, "csv") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setLabels(true); // 14.3 compatbility... to be deprecated!
        } else if (compareStringsIgnoreCase(baseFmt, "csv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setCoords(true);
            xParms->setLabels(true); // 14.3 compatbility... to be deprecated!
        } else if (compareStringsIgnoreCase(baseFmt, "lcsv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setCoords(true).setCompat(true).setLabels(true);
        } else if (compareStringsIgnoreCase(baseFmt, "dcsv") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setCoords(true).setCompat(true).setLabels(true)
                .setPretty(true);
        } else if (compareStringsIgnoreCase(baseFmt, "tsv") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setDelim('\t');
            xParms->setLabels(true); // 14.3 compatbility... to be deprecated!
        } else if (compareStringsIgnoreCase(baseFmt, "tsv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setDelim('\t').setCoords(true);
            xParms->setLabels(true); // 14.3 compatbility... to be deprecated!
        } else if (compareStringsIgnoreCase(baseFmt, "ltsv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setDelim('\t').setCoords(true).setCompat(true)
                .setLabels(true);
        }

        try {
            if (xParms.get()) {
                xParms->setParallel(flags & F_PARALLEL);
                xParms->setPrecision(ArrayWriter::getPrecision());
                n = saveXsvFormat(array, desc, f, *xParms);
            }
            else if (compareStringsIgnoreCase(format, "lsparse") == 0) {
                n = saveLsparseFormat(array, desc, f, format);
            }
#ifndef SCIDB_CLIENT
            else if (compareStringsIgnoreCase(format, "opaque") == 0) {
                n = saveOpaque(array, desc, f, query);
            }
            else if (format[0] == '(') {
                n = saveUsingTemplate(array, desc, f, format, query);
            }
#endif
            else {
                n = saveTextFormat(array, desc, f, format);
            }
        }
        catch (AwIoError& e) {
            if (f == stdout || f == stderr) {
                ::fflush(f);
            } else {
                ::fclose(f);
            }
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << ::strerror(e.error) << e.error;
        }

        int rc(0);
        if (f == stdout || f == stderr) {
            rc = ::fflush(f);
        } else {
            rc = ::fclose(f);
        }
        if (rc != 0) {
            int err = errno ? errno : EIO;
            assert(err!= EBADF);
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << ::strerror(err) << err;
        }
        return n;
    }

    const char* ArrayWriter::isSupportedFormat(const string& format)
    {
        const char* fmt = format.c_str();
        if (fmt && fmt[0] == '(') {
            // A "template" format.  Fine, whatever.
            return fmt;
        }

        // If option characters are specified (e.g. "tsv:N"), strip
        // them.  We only want to check that the base format is
        // supported.
        string baseFormat(format);
        string::size_type colon = baseFormat.find(':');
        if (colon != string::npos) {
            baseFormat = baseFormat.substr(0, colon);
        }
        fmt = baseFormat.c_str();

        // Scan format table for a match.
        for (unsigned i = 0; i < NUM_FORMATS; ++i) {
            if (strcasecmp(fmt, supportedFormats[i]) == 0)
                return supportedFormats[i];
        }
        return NULL;
    }
}

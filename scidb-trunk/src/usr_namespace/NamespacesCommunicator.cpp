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
 * Security.cpp
 *
 *  Created on: May 19, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <usr_namespace/NamespacesCommunicator.h>


#include <boost/assign.hpp>
#include <log4cxx/logger.h>
#include <memory>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Query.h>
#include <string>
#include <util/PluginManager.h>
#include <util/session/Session.h>
#include <usr_namespace/NamespaceDesc.h>

namespace scidb
{
    namespace namespaces
    {
        void Communicator::getNamespaceId(
            NamespaceDesc &             namespaceDesc,
            scidb::NamespaceDesc::ID &  namespaceID)
        {
            namespaceID = namespaceDesc.getId();
            if(!namespaceDesc.isIdInitialized())
            {
                SystemCatalog::getInstance()->findNamespace(
                    namespaceDesc.getName(),
                    namespaceID,
                    false);  // do not throw on error

                namespaceDesc.setId(namespaceID);
            }
        }

        std::string Communicator::getNamespaceName(
            const std::shared_ptr<Query>& query)
        {
            return query ? query->getNamespaceName() : "public";
        }

        void Communicator::checkArrayAccess(
            const std::string &namespaceName,
            const std::string &arrayName )
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_checkArrayAccess",    // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   pNamespaceName
                    (TID_BINARY),       //   pArrayName
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );
            if(!func.getFuncPtr())
            {
                if(!SystemCatalog::getInstance()->containsArray(arrayName))
                {
                    throw SYSTEM_EXCEPTION(
                        SCIDB_SE_SYSCAT,
                        SCIDB_LE_ARRAY_DOESNT_EXIST)
                        << arrayName;
                }
                return;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY))};   // pArrayName

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            const std::string *pArrayName = &arrayName;
            inputParams[1].setData(&pArrayName, sizeof(pArrayName));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
        }

        bool Communicator::findNamespace(
            const NamespaceDesc &       namespaceDesc,
            NamespaceDesc::ID &         namespaceID,
            bool                        throwOnErr /* = true */)
        {
            try
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "_findNamespace",       // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)                     //   in  - const NamespaceDesc *
                        (TID_BINARY),                    //   out - NamespaceDesc::ID *
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );
                if(!func.getFuncPtr())
                {
                    if(namespaceDesc.getName() == "public")
                    {
                        namespaceID =
                            scidb::NamespaceDesc::getPublicNamespaceId();

                        return true;
                    }

                    return false;
                }

                Value inputParams[] = {
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY))};


                const NamespaceDesc *   pNamespaceDesc  = &namespaceDesc;
                NamespaceDesc::ID *     pNamespaceId    = &namespaceID;

                inputParams[0].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
                inputParams[1].setData(&pNamespaceId,   sizeof(pNamespaceId));

                const Value* vInputParams[] = {
                    &inputParams[0],
                    &inputParams[1]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            } catch(Exception& e) {
                LOG4CXX_ERROR(logger, "findNamespace exception=" << e.what());

                if(throwOnErr)
                {
                    throw;
                }

                return false;
            }
        }

        bool Communicator::getNamespaces(
            std::vector<NamespaceDesc> &    namespaces)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getNamespaces",         // const std::string& name
                boost::assign::list_of    // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY),         //   out - std::vector<NamespaceDesc> *
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                // Cannot throw an exception here or
                // list('namespaces') will fail.
                const NamespaceDesc publicNS( "public",
                    scidb::NamespaceDesc::getPublicNamespaceId());
                namespaces.push_back(publicNS);

                return true;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY))}; // pNamespaceDesc


            std::vector<NamespaceDesc> *pNamespaces = &namespaces;
            inputParams[0].setData(&pNamespaces,    sizeof(pNamespaces));

            const Value* vInputParams[] = {
                &inputParams[0]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            // If the return from libnamespaces.checkArrayAccess is 0
            // then it succeeded.  Otherwise, it failed.
            int retval = returnParams.getInt32();
            return ((0 == retval) ? true : false);
        }

        bool Communicator::getRoles(
            std::vector<scidb::RoleDesc> &  roleDescs)
        {
            scidb::RoleDesc roleDesc;
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getRoles",            // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY),       //   out - const std::vector<scidb::RoleDesc> * pRoleDescs
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                LOG4CXX_DEBUG(logger, "Unable to find fn getRoles");
                return false;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY))}; // pRoleDescs

            std::vector<scidb::RoleDesc> *pRoleDescs = &roleDescs;
            inputParams[0].setData(&pRoleDescs, sizeof(pRoleDescs));

            const Value* vInputParams[] = {
                &inputParams[0]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            int retval = returnParams.getInt32();
            return (0 == retval);
        }

        bool Communicator::findNamespaceWithTransaction(
            const NamespaceDesc &       namespaceDesc,
            NamespaceDesc::ID &         namespaceID,
            pqxx::connection *          connection,
            pqxx::basic_transaction *   tr)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_findNamespaceWithTransaction",      // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)                     //   in  - const NamespaceDesc *
                    (TID_BINARY)                     //   out - NamespaceDesc::ID *
                    (TID_BINARY)                     //   in  - pqxx::connnection *
                    (TID_BINARY),                    //   in  - pqxx::basic_transaction *
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );
            if(!func.getFuncPtr())
            {
                return false;
            }

            Value inputParams[4] = {
                Value(TypeLibrary::getType(TID_BINARY)),
                Value(TypeLibrary::getType(TID_BINARY)),
                Value(TypeLibrary::getType(TID_BINARY)),
                Value(TypeLibrary::getType(TID_BINARY))};


            const NamespaceDesc *   pNamespaceDesc  = &namespaceDesc;
            NamespaceDesc::ID *     pNamespaceId    = &namespaceID;

            inputParams[0].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
            inputParams[1].setData(&pNamespaceId,   sizeof(pNamespaceId));
            inputParams[2].setData(&connection,     sizeof(connection));
            inputParams[3].setData(&tr,             sizeof(tr));

            const Value* vInputParams[4] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            // If the return from libnamespaces.checkArrayAccess is 0
            // then it succeeded.  Otherwise, it failed.
            int retval = returnParams.getInt32();
            return ((0 == retval) ? true : false);
        }


        bool Communicator::addArrayToNamespaceWithTransaction(
            const NamespaceDesc &       namespaceDesc,
            NamespaceDesc::ID &         namespaceID,
            const std::string &         arrayName,
            const ArrayID               arrayId,
            pqxx::connection *          connection,
            pqxx::basic_transaction *   tr)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_addArrayToNamespaceWithTransaction", // const std::string& name
                boost::assign::list_of    // const std::vector<TypeId>& inputArgTypes
                    (TID_INT64)             //   in - NamespaceDesc::ID     namespaceId
                    (TID_UINT64)            //   in - const ArrayID         arrayId
                    (TID_BINARY)            //   in - const NamespaceDesc * pNamespaceDesc
                    (TID_BINARY)            //   in - const std::string *   pArrayName
                    (TID_BINARY)            //   in - pqxx::connection *        connection
                    (TID_BINARY),           //   in - pqxx::basic_transaction * tr
                func,                     // FunctionDescription& funcDescription
                convs,                    // std::vector<FunctionPointer>& converters
                false);                   // bool tile );

            if(!func.getFuncPtr())
            {
                return false;
            }

            Value inputParams[6] = {
                Value(TypeLibrary::getType(TID_INT64)),   // namespaceId
                Value(TypeLibrary::getType(TID_UINT64)),  // arrayId
                Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                Value(TypeLibrary::getType(TID_BINARY)),  // pArrayName
                Value(TypeLibrary::getType(TID_BINARY)),  // connection
                Value(TypeLibrary::getType(TID_BINARY))}; // tr

            const NamespaceDesc *   pNamespaceDesc  = &namespaceDesc;
            const std::string *     pArrayName      = &arrayName;
            inputParams[0].setInt64(namespaceID);
            inputParams[1].setUint64(arrayId);
            inputParams[2].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
            inputParams[3].setData(&pArrayName,     sizeof(pArrayName));
            inputParams[4].setData(&connection,     sizeof(connection));
            inputParams[5].setData(&tr,             sizeof(tr));

            const Value* vInputParams[6] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3],
                &inputParams[4],
                &inputParams[5]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            // If the return from libnamespaces.checkArrayAccess is 0
            // then it succeeded.  Otherwise, it failed.
            int retval = returnParams.getInt32();
            return ((0 == retval) ? true : false);
        }

        void Communicator::checkNamespacePermissions(
            const std::shared_ptr<scidb::Session> &         session,
            const NamespaceDesc &                           namespaceDesc,
            const std::string &                             permissions)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_checkNamespacePermissions",      // const std::string& name
                boost::assign::list_of      // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)            //   in - const std::shared_ptr<scidb::Session> *
                    (TID_BINARY)            //   in - const NamespaceDesc * pNamespaceDesc
                    (TID_BINARY),           //   in - std::string *
                func,                       // FunctionDescription& funcDescription
                convs,                      // std::vector<FunctionPointer>& converters
                false);                     // bool tile );

            if(!func.getFuncPtr())
            {
                return;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pSession
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceDesc
                Value(TypeLibrary::getType(TID_BINARY))};   // pPermissions


            const std::shared_ptr<scidb::Session> * pSession        = &session;
            const NamespaceDesc *                   pNamespaceDesc  = &namespaceDesc;
            const std::string *                     pPermissions    = &permissions;

            inputParams[0].setData(&pSession,       sizeof(pSession));
            inputParams[1].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
            inputParams[2].setData(&pPermissions,   sizeof(pPermissions));


            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
        }

        void Communicator::getArrayDesc(
            const std::string&      namespaceName,
            const std::string&      arrayName,
            const ArrayID           catalogVersion,
            ArrayDesc&              arrayDesc)
        {
            if(ArrayDesc::isQualifiedArrayName(arrayName))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ARRAY_NAME_ALREADY_QUALIFIED)
                      << namespaceName << arrayName;
            }

            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getArrayDesc",      // const std::string& name
                boost::assign::list_of      // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)            //   in - pNamespaceName
                    (TID_BINARY)            //   in - pArrayName
                    (TID_BINARY)            //   in - pCatalogVersion
                    (TID_BINARY),           //   out - pArrayDesc
                func,                       // FunctionDescription& funcDescription
                convs,                      // std::vector<FunctionPointer>& converters
                false);                     // bool tile );

            if(!func.getFuncPtr())
            {
                SystemCatalog::getInstance()->getArrayDesc(arrayName, catalogVersion, arrayDesc);
                return;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY)),    // pArrayName
                Value(TypeLibrary::getType(TID_BINARY)),    // pCatalogVersion
                Value(TypeLibrary::getType(TID_BINARY))};   // pArrayDesc


            const std::string * pNamespaceName      = &namespaceName;
            const std::string * pArrayName          = &arrayName;
            const ArrayID *     pCatalogVersion     = &catalogVersion;
            ArrayDesc *         pArrayDesc          = &arrayDesc;

            inputParams[0].setData(&pNamespaceName,     sizeof(pNamespaceName));
            inputParams[1].setData(&pArrayName,         sizeof(pArrayName));
            inputParams[2].setData(&pCatalogVersion,    sizeof(pCatalogVersion));
            inputParams[3].setData(&pArrayDesc,         sizeof(pArrayDesc));


            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
        }

        bool Communicator::getArrayDesc(
            const std::string&      namespaceName,
            const std::string &     arrayName,
            const ArrayID           catalogVersion,
            ArrayDesc &             arrayDesc,
            const bool              throwException)
        {
            try {
                getArrayDesc(namespaceName, arrayName, catalogVersion, arrayDesc);
            } catch (const Exception& e) {
                if (!throwException &&
                    e.getLongErrorCode() == SCIDB_LE_ARRAY_DOESNT_EXIST) {
                    return false;
                }
                throw;
            }
            return true;
        }

        bool Communicator::getArrayDesc(
            const std::string &     namespaceName,
            const std::string &     arrayName,
            const ArrayID           catalogVersion,
            VersionID               arrayVersion,
            ArrayDesc &             arrayDesc,
            const bool              throwException /* = true */)
        {
            if(ArrayDesc::isQualifiedArrayName(arrayName))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ARRAY_NAME_ALREADY_QUALIFIED)
                      << namespaceName << arrayName;
            }

            LOG4CXX_TRACE(logger, "Communicator::getArrayDesc("
                << " namespaceName= " << namespaceName
                << ", arrayName= " << arrayName
                << ", arrayVersion="<< arrayVersion
                << ", catlogVersion="<< catalogVersion
                << ")");
            std::stringstream ss;

            if (arrayVersion != LAST_VERSION)
            {
                //SCIDB_ASSERT(arrayVersion>0);
                ss << arrayName << "@" << arrayVersion;
                LOG4CXX_TRACE(logger, "Communicator::getArrayDesc(): "
                    << " namespaceName = " << namespaceName
                    << " arrayName= " << ss.str());

                bool ret = getArrayDesc(
                    namespaceName, ss.str(), catalogVersion, arrayDesc, throwException);
                return ret;
            }

            LOG4CXX_TRACE(logger, "Communicator::getArrayDesc(): "
                << " namespaceName = " << namespaceName
                << " arrayName= " << ss.str());

            bool rc = getArrayDesc(
                namespaceName, arrayName, catalogVersion, arrayDesc, throwException);
            if (!rc) {
                return false;
            }

            arrayVersion = SystemCatalog::getInstance()->getLastVersion(
                arrayDesc.getId(), catalogVersion);
            if (arrayVersion == 0) {
                return true;
            }

            ss << arrayName << "@" << arrayVersion;
            LOG4CXX_TRACE(logger, "Communicator::getArrayDesc(): "
                << " namespaceName = " << namespaceName
                << " arrayName= " << ss.str());

            rc = getArrayDesc(
                namespaceName, ss.str(), catalogVersion, arrayDesc, throwException);
            return rc;
        }

        void Communicator::getArrays(
            const std::string &         namespaceName,
            std::vector<ArrayDesc> &    arrays,
            bool                        ignoreOrphanAttributes,
            bool                        ignoreVersions)
        {
            scidb::RoleDesc roleDesc;
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getArrays",           // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   in  - pNamespaceName
                    (TID_BINARY)        //   out - pArrays
                    (TID_BINARY)        //   in  - pIgnoreOrphanAttributes
                    (TID_BINARY),       //   in  - pIgnoreVersions
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                SystemCatalog::getInstance()->getArrays(
                    arrays, ignoreOrphanAttributes, ignoreVersions);
                return;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY)),    // pArrays
                Value(TypeLibrary::getType(TID_BINARY)),    // pIgnoreOrphanAttributes
                Value(TypeLibrary::getType(TID_BINARY))};   // pIgnoreVersions

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            std::vector<ArrayDesc> *pArrays = &arrays;
            inputParams[1].setData(&pArrays, sizeof(pArrays));

            bool *pIgnoreOrphanAttributes = &ignoreOrphanAttributes;
            inputParams[2].setData(&pIgnoreOrphanAttributes, sizeof(pIgnoreOrphanAttributes));

            bool *pIgnoreVersions = &ignoreVersions;
            inputParams[3].setData(&pIgnoreVersions, sizeof(pIgnoreVersions));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
        }

        ArrayID Communicator::getArrayId(
            const std::string &         namespaceName,
            const std::string &         arrayName)
        {
            ArrayID arrayId;

            scidb::RoleDesc roleDesc;
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getArrayId",           // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   in  - pNamespaceName
                    (TID_BINARY)        //   in  - pArrayName
                    (TID_BINARY),       //   out - pArrayId
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                return SystemCatalog::getInstance()->getArrayId(arrayName);
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY)),    // pArrayName
                Value(TypeLibrary::getType(TID_BINARY))};   // pArrayId

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            const std::string *pArrayName = &arrayName;
            inputParams[1].setData(&pArrayName, sizeof(pArrayName));

            ArrayID * pArrayId = &arrayId;
            inputParams[2].setData(&pArrayId, sizeof(pArrayId));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
            return arrayId;
        }

        bool Communicator::containsArray(
            const std::string &         namespaceName,
            const std::string &         arrayName)
        {
            return (getArrayId(namespaceName, arrayName) != INVALID_ARRAY_ID);
        }

        void Communicator::renameArray(
            const std::string &         namespaceName,
            const std::string &         oldArrayName,
            const std::string &         newArrayName)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_renameArray",         // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   in  - pNamespaceName
                    (TID_BINARY)        //   in  - pOldArrayName
                    (TID_BINARY),       //   in  - pNewArrayName
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                SystemCatalog::getInstance()->renameArray(oldArrayName, newArrayName);
                return ;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY)),    // pOldArrayName
                Value(TypeLibrary::getType(TID_BINARY))};   // pNewArrayName

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            const std::string *pOldArrayName = &oldArrayName;
            inputParams[1].setData(&pOldArrayName, sizeof(pOldArrayName));

            const std::string *pNewArrayName = &newArrayName;
            inputParams[2].setData(&pNewArrayName, sizeof(pNewArrayName));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);
        }

        bool Communicator::deleteArray(
            const std::string &         namespaceName,
            const std::string &         arrayName)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_deleteArray",         // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   in  - pNamespaceName
                    (TID_BINARY),       //   in  - pArrayName
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                return SystemCatalog::getInstance()->deleteArray(arrayName);
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY))};   // pArrayName

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            const std::string *pArrayName = &arrayName;
            inputParams[1].setData(&pArrayName, sizeof(pArrayName));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            try
            {
                func.getFuncPtr()(vInputParams, &returnParams, NULL);
            } catch (const Exception& e) {
                LOG4CXX_ERROR(logger, "Communicator::deleteArray exception=" << e.what());
                return false;
            }
            return true;
        }

        bool Communicator::deleteArrayVersions(
            const std::string &         namespaceName,
            const std::string &         arrayName,
            const VersionID             arrayVersion)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_deleteArrayVersions", // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)        //   in  - pNamespaceName
                    (TID_BINARY)        //   in  - pArrayName
                    (TID_BINARY),       //   in  - pArrayVersion
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                return SystemCatalog::getInstance()->deleteArrayVersions(arrayName, arrayVersion);
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pNamespaceName
                Value(TypeLibrary::getType(TID_BINARY)),    // pArrayName
                Value(TypeLibrary::getType(TID_BINARY))};   // pArrayVersion

            const std::string *pNamespaceName = &namespaceName;
            inputParams[0].setData(&pNamespaceName, sizeof(pNamespaceName));

            const std::string *pArrayName = &arrayName;
            inputParams[1].setData(&pArrayName, sizeof(pArrayName));

            const VersionID *pArrayVersion = &arrayVersion;
            inputParams[2].setData(&pArrayVersion, sizeof(pArrayVersion));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            try
            {
                func.getFuncPtr()(vInputParams, &returnParams, NULL);
            } catch (const Exception& e) {
                LOG4CXX_ERROR(logger, "Communicator::deleteArrayVersions exception=" << e.what());
                return false;
            }
            return true;
        }

    } // namespace namespaces
} // namespace scidb

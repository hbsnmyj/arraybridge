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
 * @file Cluster.h
 *
 * @brief Contains class for providing information about cluster
 *
 * @author roman.simakov@gmail.com
 */
#include <limits>
#include <string>
#include <memory>
#include <system/Cluster.h>
#include <array/Metadata.h>
#include <network/NetworkManager.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{
const double InstanceDesc::INFINITY_TS = std::numeric_limits<double>::infinity();

Cluster::Cluster()
: _uuid(SystemCatalog::getInstance()->getClusterUuid())
{
}

InstMembershipPtr
Cluster::getInstanceMembership(MembershipID id)
{
   ScopedMutexLock lock(_mutex);

   if (!_lastMembership || _lastMembership->getId() < id) {
       getInstances();
   }
   SCIDB_ASSERT(_lastMembership);

   return _lastMembership;
}

void Cluster::getInstances()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    Instances instances;
    MembershipID id = SystemCatalog::getInstance()->getInstances(instances);
    SCIDB_ASSERT(!instances.empty());
    _lastMembership = std::make_shared<InstanceMembership>(id, instances);
    SCIDB_ASSERT(instances.empty());
}

namespace
{
    /// A functor class that adds a given instance to a liveness object
    class LivenessConstructor
    {
    public:
        LivenessConstructor(InstanceLiveness* newLiveness)
        :  _newLiveness(newLiveness)
        {}
        void operator() (const InstanceDesc& i)
        {
            const InstanceID instanceId = i.getInstanceId();
            InstanceLivenessEntry entry(instanceId, 0, false);
            _newLiveness->insert(&entry);
        }
    private:
        InstanceLiveness* _newLiveness;
    };
}

InstLivenessPtr Cluster::getInstanceLiveness()
{
   InstLivenessPtr liveness(NetworkManager::getInstance()->getInstanceLiveness());
   if (liveness) {
      return liveness;
   }
   InstMembershipPtr membership(getInstanceMembership(0));
   SCIDB_ASSERT(membership);

   std::shared_ptr<InstanceLiveness> newLiveness(new InstanceLiveness(membership->getId(), 0));
   LivenessConstructor lc(newLiveness.get());
   membership->visitInstances(lc);

   liveness = newLiveness;
   SCIDB_ASSERT(liveness->getNumLive() > 0);
   return liveness;
}

InstanceID Cluster::getLocalInstanceId()
{
   return NetworkManager::getInstance()->getPhysicalInstanceID();
}

InstanceDesc::InstanceDesc(const std::string& host,
                           uint16_t port,
                           const std::string& basePath,
                           uint32_t serverId,
                           uint32_t serverInstanceId)
        : _instanceId(INVALID_INSTANCE),
          _membershipId(0),
          _host(host),
          _port(port),
          _online(INFINITY_TS),
          _basePath(basePath),
          _serverId(serverId),
          _serverInstanceId(serverInstanceId)
{
    boost::filesystem::path p(_basePath);
    _basePath = p.normalize().string();
    SCIDB_ASSERT(!_host.empty());
    SCIDB_ASSERT(!_basePath.empty());
}

InstanceDesc::InstanceDesc(InstanceID instanceId,
                           MembershipID membershipId,
                           const std::string& host,
                           uint16_t port,
                           double onlineTs,
                           const std::string& basePath,
                           uint32_t serverId,
                           uint32_t serverInstanceId)
        : _instanceId(instanceId),
          _membershipId(membershipId),
          _host(host),
          _port(port),
          _online(onlineTs),
          _basePath(basePath),
          _serverId(serverId),
          _serverInstanceId(serverInstanceId)
{
    boost::filesystem::path p(_basePath);
    _basePath = p.normalize().string();
    SCIDB_ASSERT(!_host.empty());
    SCIDB_ASSERT(!_basePath.empty());
}

std::string InstanceDesc::getPath() const
{
    std::stringstream ss;
    // XXX TODO: consider boost path utils
    ss << _basePath << "/" << _serverId << "/" << _serverInstanceId ;
    return ss.str();
}

std::ostream& operator<<(std::ostream& stream,const InstanceDesc& instance)
{
    stream << "instance { id = " << instance.getInstanceId()
           << ", membership_id = " << instance.getMembershipId()
           << ", host = " << instance.getHost()
           << ", port = " << instance.getPort()
           << ", member since " << instance.getOnlineSince()
           << ", base_path = " << instance.getPath()
           << ", server_id = " << instance.getServerId()
           << ", server_instance_id = " << instance.getServerInstanceId()
           <<" }";
    return stream;
}

} // namespace scidb

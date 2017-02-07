////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Kaveh Vahedipour
////////////////////////////////////////////////////////////////////////////////

#include "Supervision.h"

#include <thread>

#include "Agency/AddFollower.h"
#include "Agency/Agent.h"
#include "Agency/CleanOutServer.h"
#include "Agency/FailedFollower.h"
#include "Agency/FailedLeader.h"
#include "Agency/FailedServer.h"
#include "Agency/Job.h"
#include "Agency/MoveShard.h"
#include "Agency/RemoveServer.h"
#include "Agency/Store.h"
#include "Agency/UnassumedLeadership.h"
#include "ApplicationFeatures/ApplicationServer.h"
#include "Basics/ConditionLocker.h"
#include "Basics/MutexLocker.h"

using namespace arangodb;

using namespace arangodb::consensus;
using namespace arangodb::application_features;

std::string Supervision::_agencyPrefix = "/arango";

Supervision::Supervision()
    : arangodb::Thread("Supervision"),
      _agent(nullptr),
      _snapshot("Supervision"),
      _transient("Transient"),
      _frequency(1.),
      _gracePeriod(5.),
      _jobId(0),
      _jobIdMax(0),
      _selfShutdown(false) {}

Supervision::~Supervision() { shutdown(); };

static std::string const syncPrefix = "/Sync/ServerStates/";
static std::string const healthPrefix = "/Supervision/Health/";
static std::string const planDBServersPrefix = "/Plan/DBServers";
static std::string const planCoordinatorsPrefix = "/Plan/Coordinators";
static std::string const targetShortID = "/Target/MapUniqueToShortID/";
static std::string const currentServersRegisteredPrefix =
    "/Current/ServersRegistered";
static std::string const foxxmaster = "/Current/Foxxmaster";

// Upgrade agency, guarded by wakeUp
void Supervision::upgradeAgency() {
  try {
    if (_snapshot(failedServersPrefix).slice().isArray()) {
      Builder builder;
      builder.openArray();
      builder.openObject();
      builder.add(
        _agencyPrefix + failedServersPrefix, VPackValue(VPackValueType::Object));
      for (auto const& failed :
             VPackArrayIterator(_snapshot(failedServersPrefix).slice())) {
        builder.add(failed.copyString(), VPackValue(VPackValueType::Object));
        builder.close();
      }
      builder.close();
      builder.close();
      builder.close();
      transact(_agent, builder);
    }
  } catch (std::exception const&) {
    Builder builder;
    builder.openArray();
    builder.openObject();
    builder.add(
      _agencyPrefix + failedServersPrefix, VPackValue(VPackValueType::Object));
    builder.close();
    builder.close();
    builder.close();
    transact(_agent, builder);    
  }
}

// Check all DB servers, guarded above doChecks
std::vector<check_t> Supervision::checkDBServers() {

  std::vector<check_t> ret;
  auto const& machinesPlanned =
      _snapshot(planDBServersPrefix).children();
  auto const& serversRegistered =
      _snapshot(currentServersRegisteredPrefix).children();

  std::vector<std::string> todelete;
  for (auto const& machine : _snapshot(healthPrefix).children()) {
    if (machine.first.substr(0, 2) == "DB") {
      todelete.push_back(machine.first);
    }
  }

  for (auto const& machine : machinesPlanned) {
    bool good = false;
    bool reportPersistent = false;
    std::string lastHeartbeatTime, lastHeartbeatAcked, lastStatus,
        heartbeatTime, heartbeatStatus, serverID;

    serverID = machine.first;
    heartbeatTime = _transient(syncPrefix + serverID + "/time").toJson();
    heartbeatStatus = _transient(syncPrefix + serverID + "/status").toJson();

    todelete.erase(std::remove(todelete.begin(), todelete.end(), serverID),
                   todelete.end());

    std::string shortName = "Unknown";
    try {
      shortName = _snapshot(targetShortID + serverID + "/ShortName").toJson();
    } catch (...) {} 

    try {  // Existing
      lastHeartbeatTime =
        _transient(healthPrefix + serverID + "/LastHeartbeatSent").toJson();
      lastHeartbeatAcked =
        _transient(healthPrefix + serverID + "/LastHeartbeatAcked").toJson();
      lastStatus = _transient(healthPrefix + serverID + "/Status").toJson();
      if (lastHeartbeatTime != heartbeatTime) {  // Update
        good = true;
      }
    } catch (...) {  // New server
      good = true;
    }

    auto report = std::make_shared<Builder>();
    report->openArray();
    report->openArray();
    report->openObject();
    report->add(_agencyPrefix + healthPrefix + serverID,
                VPackValue(VPackValueType::Object));
    report->add("LastHeartbeatSent", VPackValue(heartbeatTime));
    report->add("LastHeartbeatStatus", VPackValue(heartbeatStatus));
    report->add("Role", VPackValue("DBServer"));
    report->add("ShortName", VPackValue(shortName));
    auto endpoint = serversRegistered.find(serverID);
    if (endpoint != serversRegistered.end()) {
      endpoint = endpoint->second->children().find("endpoint");
      if (endpoint != endpoint->second->children().end()) {
        if (endpoint->second->children().size() == 0) {
          VPackSlice epString = endpoint->second->slice();
          if (epString.isString()) {
            report->add("Endpoint", epString);
          }
        }
      }
    }

    if (good) {
      if (lastStatus != Supervision::HEALTH_STATUS_GOOD) {
        reportPersistent = true;
      }
      report->add(
        "LastHeartbeatAcked",
        VPackValue(timepointToString(std::chrono::system_clock::now())));
      report->add("Status", VPackValue(Supervision::HEALTH_STATUS_GOOD));
      
      std::string failedServerPath = failedServersPrefix + "/" + serverID;
      if (_snapshot.exists(failedServerPath).size() == 3) {
        Builder del;
        del.openArray();
        del.openObject();
        del.add(_agencyPrefix + failedServerPath,
                VPackValue(VPackValueType::Object));
        del.add("op", VPackValue("delete"));
        del.close();
        del.close();
        del.close();
        transact(_agent, del);
      }
      
    } else {
      
      auto elapsed = std::chrono::duration<double>(
        std::chrono::system_clock::now() -
        stringToTimepoint(lastHeartbeatAcked)).count();
      
      auto secondsSinceLeader = std::chrono::duration<double>(
        std::chrono::system_clock::now() - _agent->leaderSince()).count();
      
      // Failed servers are considered only after having taken on leadership
      // for at least grace period
      if (elapsed > _gracePeriod && secondsSinceLeader > _gracePeriod) {
        if (lastStatus == Supervision::HEALTH_STATUS_BAD) {
          reportPersistent = true;
          report->add("Status", VPackValue(Supervision::HEALTH_STATUS_FAILED));
          FailedServer fsj(_snapshot, _agent, std::to_string(_jobId++),
                           "supervision", _agencyPrefix, serverID);
        }
      } else {
        report->add("Status", VPackValue(Supervision::HEALTH_STATUS_BAD));
      }
      
    }

    report->close();
    report->close();
    report->close();
    report->close();
    
    if (!this->isStopping()) {
      _agent->transient(report);
      if (reportPersistent) {
        _agent->write(report);
      }
    }
    
  }

  if (!todelete.empty()) {
    query_t del = std::make_shared<Builder>();
    del->openArray();
    del->openArray();
    del->openObject();
    for (auto const& srv : todelete) {
      del->add(_agencyPrefix + healthPrefix + srv,
               VPackValue(VPackValueType::Object));
      del->add("op", VPackValue("delete"));
      del->close();
    }
    del->close();
    del->close();
    del->close();
    _agent->write(del);
  }

  return ret;
}

// Check all coordinators, guarded above doChecks
std::vector<check_t> Supervision::checkCoordinators() {

  std::vector<check_t> ret;
  auto const& machinesPlanned =
      _snapshot(planCoordinatorsPrefix).children();
  auto const& serversRegistered =
      _snapshot(currentServersRegisteredPrefix).children();

  std::string currentFoxxmaster;
  try {
    currentFoxxmaster = _snapshot(foxxmaster).getString();
  } catch (...) {}

  std::string goodServerId;
  bool foxxmasterOk = false;
  std::vector<std::string> todelete;
  for (auto const& machine : _snapshot(healthPrefix).children()) {
    if (machine.first.substr(0, 2) == "Co") {
      todelete.push_back(machine.first);
    }
  }

  for (auto const& machine : machinesPlanned) {
    bool reportPersistent = false;
    bool good = false;
    std::string lastHeartbeatTime, lastHeartbeatAcked, lastStatus,
        heartbeatTime, heartbeatStatus, serverID;

    serverID = machine.first;
    heartbeatTime = _transient(syncPrefix + serverID + "/time").toJson();
    heartbeatStatus = _transient(syncPrefix + serverID + "/status").toJson();

    todelete.erase(std::remove(todelete.begin(), todelete.end(), serverID),
                   todelete.end());

    std::string shortName = "Unknown";
    try {
      shortName = _snapshot(targetShortID + serverID + "/ShortName").toJson();
    } catch (...) {} 

    try {  // Existing
      lastHeartbeatTime =
        _transient(healthPrefix + serverID + "/LastHeartbeatSent").toJson();
      lastHeartbeatAcked =
        _transient(healthPrefix + serverID + "/LastHeartbeatAcked").toJson();
      lastStatus = _transient(healthPrefix + serverID + "/Status").toJson();
      if (lastHeartbeatTime != heartbeatTime) {  // Update
        good = true;
      }
    } catch (...) {  // New server
      good = true;
    }

    query_t report = std::make_shared<Builder>();
    report->openArray();
    report->openArray();
    report->openObject();
    report->add(_agencyPrefix + healthPrefix + serverID,
                VPackValue(VPackValueType::Object));
    report->add("LastHeartbeatSent", VPackValue(heartbeatTime));
    report->add("LastHeartbeatStatus", VPackValue(heartbeatStatus));
    report->add("Role", VPackValue("Coordinator"));
    report->add("ShortName", VPackValue(shortName));
    auto endpoint = serversRegistered.find(serverID);
    if (endpoint != serversRegistered.end()) {
      endpoint = endpoint->second->children().find("endpoint");
      if (endpoint != endpoint->second->children().end()) {
        if (endpoint->second->children().size() == 0) {
          VPackSlice epString = endpoint->second->slice();
          if (epString.isString()) {
            report->add("Endpoint", epString);
          }
        }
      }
    }

    if (good) {
      if (lastStatus != Supervision::HEALTH_STATUS_GOOD) {
        reportPersistent = true;
      }
      if (goodServerId.empty()) {
        goodServerId = serverID;
      }
      if (serverID == currentFoxxmaster) {
        foxxmasterOk = true;
      }
      report->add(
          "LastHeartbeatAcked",
          VPackValue(timepointToString(std::chrono::system_clock::now())));
      report->add("Status", VPackValue(Supervision::HEALTH_STATUS_GOOD));
    } else {

      auto elapsed = std::chrono::duration<double>(
        std::chrono::system_clock::now() -
        stringToTimepoint(lastHeartbeatAcked)).count();
      
      auto secondsSinceLeader = std::chrono::duration<double>(
        std::chrono::system_clock::now() - _agent->leaderSince()).count();
      
      if (elapsed > _gracePeriod && secondsSinceLeader > _gracePeriod) {
        if (lastStatus == Supervision::HEALTH_STATUS_BAD) {
          report->add("Status", VPackValue(Supervision::HEALTH_STATUS_FAILED));
          reportPersistent = true;
        }
      } else {
        report->add("Status", VPackValue(Supervision::HEALTH_STATUS_BAD));
      }
    }

    report->close();
    report->close();
    report->close();
    report->close();
    if (!this->isStopping()) {
      _agent->transient(report);
      if (reportPersistent) { // STATUS changes should be persisted
        _agent->write(report);
      }
    }
  }

  if (!todelete.empty()) {
    query_t del = std::make_shared<Builder>();
    del->openArray();
    del->openArray();
    del->openObject();
    for (auto const& srv : todelete) {
      del->add(_agencyPrefix + healthPrefix + srv,
               VPackValue(VPackValueType::Object));
      del->add("op", VPackValue("delete"));
      del->close();
    }
    del->close();
    del->close();
    del->close();
    _agent->write(del);
  }

  if (!foxxmasterOk && !goodServerId.empty()) {
    query_t create = std::make_shared<Builder>();
    create->openArray();
    create->openArray();
    create->openObject();
    create->add(_agencyPrefix + foxxmaster, VPackValue(goodServerId));
    create->close();
    create->close();
    create->close();

    _agent->write(create);
  }

  return ret;
}

// Update local agency snapshot, guarded by callers
bool Supervision::updateSnapshot() {

  if (_agent == nullptr || this->isStopping()) {
    return false;
  }
  
  try {
    _snapshot = _agent->readDB().get(_agencyPrefix);
    _transient = _agent->transient().get(_agencyPrefix);
  } catch (...) {}
  
  return true;
  
}

// All checks, guarded by main thread
bool Supervision::doChecks() {
  checkDBServers();
  checkCoordinators();
  return true;
}


void Supervision::run() {
  bool shutdown = false;
  {
    CONDITION_LOCKER(guard, _cv);
    TRI_ASSERT(_agent != nullptr);

    // Get agency prefix after cluster init
    uint64_t jobId = 0;
    {
      MUTEX_LOCKER(locker, _lock);
      jobId = _jobId;
    }
    
    if (jobId == 0) {
      // We need the agency prefix to work, but it is only initialized by
      // some other server in the cluster. Since the supervision does not
      // make sense at all without other ArangoDB servers, we wait pretty
      // long here before giving up:
      if (!updateAgencyPrefix(1000)) {
        LOG_TOPIC(DEBUG, Logger::AGENCY)
            << "Cannot get prefix from Agency. Stopping supervision for good.";
        return;
      }
    }

    while (!this->isStopping()) {

      // Get bunch of job IDs from agency for future jobs
      if (_agent->leading() && (_jobId == 0 || _jobId == _jobIdMax)) {
        getUniqueIds();  // cannot fail but only hang
      }

      {
        MUTEX_LOCKER(locker, _lock);

        updateSnapshot();
        // mop: always do health checks so shutdown is able to detect if a server
        // failed otherwise
        if (_agent->leading()) {
          upgradeAgency();
          doChecks();
        }

        if (isShuttingDown()) {
          handleShutdown();
        } else if (_selfShutdown) {
          shutdown = true;
          break;
        } else if (_agent->leading()) {
          if (!handleJobs()) {
            break;
          }
        }
      }
      _cv.wait(static_cast<uint64_t>(1000000 * _frequency));
    }
  }
  if (shutdown) {
    ApplicationServer::server->beginShutdown();
  }
}

// Guarded by caller
bool Supervision::isShuttingDown() {
  try {
    return _snapshot("/Shutdown").getBool();
  } catch (...) {
    return false;
  }
}

// Guarded by caller
std::string Supervision::serverHealth(std::string const& serverName) {
  try {
    std::string const serverStatus(healthPrefix + serverName + "/Status");
    auto const status = _snapshot(serverStatus).getString();
    return status;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::AGENCY)
        << "Couldn't read server health status for server " << serverName;
    return "";
  }
}

// Guarded by caller
void Supervision::handleShutdown() {
  _selfShutdown = true;
  LOG_TOPIC(DEBUG, Logger::AGENCY) << "Waiting for clients to shut down";
  auto const& serversRegistered =
      _snapshot(currentServersRegisteredPrefix).children();
  bool serversCleared = true;
  for (auto const& server : serversRegistered) {
    if (server.first == "Version") {
      continue;
    }

    LOG_TOPIC(DEBUG, Logger::AGENCY) << "Waiting for " << server.first
                                     << " to shutdown";

    if (serverHealth(server.first) != HEALTH_STATUS_GOOD) {
      LOG_TOPIC(WARN, Logger::AGENCY) << "Server " << server.first
                                      << " did not shutdown properly it seems!";
      continue;
    }
    serversCleared = false;
  }

  if (serversCleared) {
    if (_agent->leading()) {
      auto del = std::make_shared<Builder>();
      del->openArray();
      del->openArray();
      del->openObject();
      del->add(_agencyPrefix + "/Shutdown", VPackValue(VPackValueType::Object));
      del->add("op", VPackValue("delete"));
      del->close();
      del->close();
      del->close();
      del->close();
      auto result = _agent->write(del);
      if (result.indices.size() != 1) {
        LOG(ERR) << "Invalid resultsize of " << result.indices.size()
                 << " found during shutdown";
      } else {
        if (_agent->waitFor(result.indices.at(0)) != Agent::raft_commit_t::OK) {
          LOG(ERR) << "Result was not written to followers during shutdown";
        }
      }
    }
  }
}

// Guarded by caller 
bool Supervision::handleJobs() {
  // Do supervision
  
  shrinkCluster();
  enforceReplication();
  workJobs();

  return true;
}

// Guarded by caller
void Supervision::workJobs() {
  Node::Children const& todos = _snapshot(toDoPrefix).children();
  Node::Children const& pends = _snapshot(pendingPrefix).children();

  for (auto const& todoEnt : todos) {
    auto const& job = *todoEnt.second;

    std::string jobType = job("type").getString(),
                jobId = job("jobId").getString(),
                creator = job("creator").getString();
    if (jobType == "failedServer") {
      FailedServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "addFollower") {
      AddFollower(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "cleanOutServer") {
      CleanOutServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "removeServer") {
      RemoveServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "moveShard") {
      MoveShard(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "failedLeader") {
      FailedLeader(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "failedFollower") {
      FailedFollower(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "unassumedLeadership") {
      UnassumedLeadership(_snapshot, _agent, jobId, creator, _agencyPrefix);
    }
  }

  for (auto const& pendEnt : pends) {
    auto const& job = *pendEnt.second;

    std::string jobType = job("type").getString(),
                jobId = job("jobId").getString(),
                creator = job("creator").getString();
    if (jobType == "failedServer") {
      FailedServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "addFollower") {
      AddFollower(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "cleanOutServer") {
      CleanOutServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "removeServer") {
      RemoveServer(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "moveShard") {
      MoveShard(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "failedLeader") {
      FailedLeader(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "failedFollower") {
      FailedLeader(_snapshot, _agent, jobId, creator, _agencyPrefix);
    } else if (jobType == "unassumedLeadership") {
      UnassumedLeadership(_snapshot, _agent, jobId, creator, _agencyPrefix);
    }
  }
}

void Supervision::enforceReplication() {

  auto const& todo = _snapshot(toDoPrefix).children();
  auto const& pending = _snapshot(pendingPrefix).children();

  if (!todo.empty() || !pending.empty()) { // This is low priority
    return;
  }

  auto const& plannedDBs = _snapshot(planColPrefix).children();
  auto available = Job::availableServers(_snapshot);

  for (const auto& db_ : plannedDBs) { // Planned databases
    auto const& db = *(db_.second);
    for (const auto& col_ : db.children()) { // Planned collections
      auto const& col = *(col_.second);
      
      size_t replicationFactor;
      try {
        replicationFactor = col("replicationFactor").slice().getUInt();
      } catch (std::exception const&) {
        LOG_TOPIC(DEBUG, Logger::AGENCY)
          << "no replicationFactor entry in " << col.toJson();
        continue;
      }

      // mop: satellites => distribute to every server
      if (replicationFactor == 0) {
        replicationFactor = available.size();
      }
      
      bool clone = false;
      try {
        clone = !col("distributeShardsLike").slice().copyString().empty();
      } catch (...) {}

      auto const& failed = _snapshot(failedServersPrefix).children();

      if (!clone) {
        for (auto const& shard_ : col("shards").children()) { // Pl shards
          auto const& shard = *(shard_.second);
          
          size_t actualReplicationFactor = shard.slice().length();
          for (const auto& i : VPackArrayIterator(shard.slice())) {
            if (failed.find(i.copyString())!=failed.end()) {
              --actualReplicationFactor;
            }
          }
          
          if (actualReplicationFactor > 0 && // Need at least one survived
              replicationFactor > actualReplicationFactor && // Planned higher
              available.size() > shard.slice().length()) { // Any servers available
            for (auto const& i : VPackArrayIterator(shard.slice())) {
              available.erase(
                std::remove(
                  available.begin(), available.end(), i.copyString()),
                available.end());
            }
            
            size_t optimal = replicationFactor - actualReplicationFactor;
            std::vector<std::string> newFollowers;
            for (size_t i = 0; i < optimal; ++i) {
              auto randIt = available.begin();
              std::advance(randIt, std::rand() % available.size());
              newFollowers.push_back(*randIt);
              available.erase(randIt);
              if (available.empty()) {
                break;
              }
            }

            AddFollower(
              _snapshot, _agent, std::to_string(_jobId++), "supervision",
              _agencyPrefix, db_.first, col_.first, shard_.first, newFollowers);

          }
        }
      }
    }
  }
  
}

// Shrink cluster if applicable, guarded by caller
void Supervision::shrinkCluster() {

  auto const& todo = _snapshot(toDoPrefix).children();
  auto const& pending = _snapshot(pendingPrefix).children();

  if (!todo.empty() || !pending.empty()) { // This is low priority
    return;
  }
  
  // Get servers from plan
  auto availServers = Job::availableServers(_snapshot);

  size_t targetNumDBServers;
  try {
    targetNumDBServers = _snapshot("/Target/NumberOfDBServers").getUInt();
  } catch (std::exception const& e) {
    LOG_TOPIC(TRACE, Logger::AGENCY)
        << "Targeted number of DB servers not set yet: " << e.what();
    return;
  }

  // Only if number of servers in target is smaller than the available
  if (targetNumDBServers < availServers.size()) {
    // Minimum 1 DB server must remain
    if (availServers.size() == 1) {
      LOG_TOPIC(DEBUG, Logger::AGENCY)
          << "Only one db server left for operation";
      return;
    }

    // mop: any failed server is first considered useless and may be cleared
    // from the list later on :O
    std::vector<std::string> uselessFailedServers;
    auto failedPivot = std::partition(
        availServers.begin(), availServers.end(), [this](std::string server) {
          return serverHealth(server) != HEALTH_STATUS_FAILED;
        });
    std::move(failedPivot, availServers.end(),
              std::back_inserter(uselessFailedServers));
    availServers.erase(failedPivot, availServers.end());

    /**
     * mop: TODO instead of using Plan/Collections we should watch out for
     * Plan/ReplicationFactor and Current...when the replicationFactor is not
     * fullfilled we should add a follower to the plan
     * When seeing more servers in Current than replicationFactor we should
     * remove a server.
     * RemoveServer then should be changed so that it really just kills a server
     * after a while...
     * this way we would have implemented changing the replicationFactor and
     * have an awesome new feature
     **/
    // Find greatest replication factor among all collections
    uint64_t maxReplFact = 1;
    auto const& databases = _snapshot(planColPrefix).children();
    for (auto const& database : databases) {
      for (auto const& collptr : database.second->children()) {
        uint64_t replFact{0};
        try {
          replFact = (*collptr.second)("replicationFactor").getUInt();

          if (replFact > maxReplFact) {
            maxReplFact = replFact;
          }
        } catch (std::exception const& e) {
          LOG_TOPIC(WARN, Logger::AGENCY) << "Cannot retrieve replication "
                                          << "factor for collection "
                                          << collptr.first << ": " << e.what();
          return;
        }
        if (uselessFailedServers.size() > 0) {
          try {
            auto const& shards =
                (*collptr.second)("shards").children();
            for (auto const& shard : shards) {
              auto const& children = shard.second->children();
              for (size_t i = 0; i < children.size(); i++) {
                auto const& server =
                    children.at(std::to_string(i))->getString();
                auto found = std::find(uselessFailedServers.begin(),
                                       uselessFailedServers.end(), server);

                bool isLeader = i == 0;
                if (found != uselessFailedServers.end() &&
                    (isLeader || replFact >= availServers.size())) {
                  // mop: apparently it has been a lie :O it is not useless
                  uselessFailedServers.erase(found);
                }
              }
            }
          } catch (std::exception const& e) {
            LOG_TOPIC(WARN, Logger::AGENCY)
                << "Cannot retrieve shard information for " << collptr.first
                << ": " << e.what();
          } catch (...) {
            LOG_TOPIC(WARN, Logger::AGENCY)
                << "Cannot retrieve shard information for " << collptr.first;
          }
        }
      }
    }

    if (uselessFailedServers.size() > 0) {
      // Schedule last server for cleanout

      // cppcheck-suppress *
      RemoveServer(_snapshot, _agent, std::to_string(_jobId++), "supervision",
                   _agencyPrefix, uselessFailedServers.back());
      return;
    }
    // mop: do not account any failedservers in this calculation..the ones
    // having
    // a state of failed still have data of interest to us! We wait indefinately
    // for them to recover or for the user to remove them
    if (maxReplFact < availServers.size()) {
      // Clean out as long as number of available servers is bigger
      // than maxReplFactor and bigger than targeted number of db servers
      if (availServers.size() > maxReplFact &&
          availServers.size() > targetNumDBServers) {
        // Sort servers by name
        std::sort(availServers.begin(), availServers.end());

        // Schedule last server for cleanout
        CleanOutServer(_snapshot, _agent, std::to_string(_jobId++),
                       "supervision", _agencyPrefix, availServers.back());
      }
    }
  }
}

// Start thread
bool Supervision::start() {
  Thread::start();
  return true;
}

// Start thread with agent
bool Supervision::start(Agent* agent) {
  _agent = agent;
  _frequency = _agent->config().supervisionFrequency();
  _gracePeriod = _agent->config().supervisionGracePeriod();
  return start();
}

// Get agency prefix fron agency
bool Supervision::updateAgencyPrefix(size_t nTries, double intervalSec) {
  // Try nTries to get agency's prefix in intervals
  while (!this->isStopping()) {
    MUTEX_LOCKER(locker, _lock);
    _snapshot = _agent->readDB().get("/");
    if (_snapshot.children().size() > 0) {
      _agencyPrefix =
          "/arango";  // std::string("/") + _snapshot.children().begin()->first;
      LOG_TOPIC(DEBUG, Logger::AGENCY) << "Agency prefix is " << _agencyPrefix;
      return true;
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(intervalSec));
  }

  // Stand-alone agency
  return false;
}

static std::string const syncLatest = "/Sync/LatestID";
// Get bunch of cluster's unique ids from agency, guarded above
void Supervision::getUniqueIds() {
  uint64_t latestId;
  // Run forever, supervision does not make sense before the agency data
  // is initialized by some other server...
  while (!this->isStopping()) {
    try {
      MUTEX_LOCKER(locker, _lock);
      latestId = std::stoul(
        _agent->readDB().get(_agencyPrefix + "/Sync/LatestID").slice().toJson());
    } catch (...) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    Builder uniq;
    uniq.openArray();
    uniq.openObject();
    uniq.add(_agencyPrefix + syncLatest, VPackValue(latestId + 100000));  // new
    uniq.close();
    uniq.openObject();
    uniq.add(_agencyPrefix + syncLatest, VPackValue(latestId));  // precond
    uniq.close();
    uniq.close();

    auto result = transact(_agent, uniq);

    if (!result.accepted || result.indices.empty()) {
      return;
    }

    if (result.indices[0]) {
      _agent->waitFor(result.indices[0]);
      _jobId = latestId;
      _jobIdMax = latestId + 100000;
      return;
    }
  }
}

void Supervision::beginShutdown() {
  // Personal hygiene
  Thread::beginShutdown();

  CONDITION_LOCKER(guard, _cv);
  guard.broadcast();
}


void Supervision::missingPrototype() {

  auto const& plannedDBs = _snapshot(planColPrefix).children();
  //auto available = Job::availableServers(_snapshot);
  
  // key: prototype, value: clone
  //std::multimap<std::string, std::string> likeness;
  
  for (const auto& db_ : plannedDBs) { // Planned databases
    auto const& db = *(db_.second);
    
    for (const auto& col_ : db.children()) { // Planned collections
      auto const& col = *(col_.second);
      
      auto prototype = col("distributeShardsLike").slice().copyString();
      if (prototype.empty()) {
        continue;
      }
      
    }
  }
}

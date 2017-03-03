//////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 EMC Corporation
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasily Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "ViewFeature.h"
#include "RestViewHandler.h"

#include "Basics/ArangoGlobalContext.h"
#include "Cluster/ServerState.h"
#include "GeneralServer/GeneralServerFeature.h"
#include "GeneralServer/RestHandlerFactory.h"
#include "RestHandler/RestHandlerCreator.h"
#include "Logger/Logger.h"
#include "ProgramOptions/ProgramOptions.h"
#include "ProgramOptions/Section.h"

using namespace arangodb;
using namespace arangodb::options;

namespace {

RestViewHandler::ViewFactory localViewFactory = [] (
    arangodb::StringRef const& type,
    VPackSlice params,
    VPackBuilder* out) -> bool {
  return false;
};

RestViewHandler::ViewFactory remoteViewFactory = [] (
    arangodb::StringRef const& type,
    VPackSlice params,
    VPackBuilder* out) -> bool {
  return false;
};

}

ViewFeature::ViewFeature(application_features::ApplicationServer* server)
    : ApplicationFeature(server, "View") {
  setOptional(true);
  requiresElevatedPrivileges(false);
  startsAfter("Logger");
  // ensure that GeneralServerFeature::HANDLER_FACTORY is already initialized
  startsAfter("GeneralServer");
}

void ViewFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
}

void ViewFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
}

void ViewFeature::prepare() {
}

void ViewFeature::start() {
  GeneralServerFeature::HANDLER_FACTORY->addPrefixHandler(
    RestViewHandler::VIEW_PATH,
    RestHandlerCreator<RestViewHandler>::createData<const RestViewHandler::ViewFactory*>,
    ServerState::instance()->isCoordinator() ? &remoteViewFactory : &localViewFactory
  );
}

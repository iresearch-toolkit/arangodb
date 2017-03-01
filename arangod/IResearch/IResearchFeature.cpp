////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
/// @author Dr. Frank Celler
////////////////////////////////////////////////////////////////////////////////

#include "IResearchFeature.h"
#include "RestIResearchViewHandler.h"

#include "Basics/ArangoGlobalContext.h"
#include "GeneralServer/GeneralServerFeature.h"
#include "GeneralServer/RestHandlerFactory.h"
#include "RestHandler/RestHandlerCreator.h"
#include "Logger/Logger.h"
#include "ProgramOptions/ProgramOptions.h"
#include "ProgramOptions/Section.h"

using namespace arangodb;
using namespace arangodb::options;

IResearchFeature::IResearchFeature(application_features::ApplicationServer* server)
    : ApplicationFeature(server, "IResearch") {
  setOptional(true);
  requiresElevatedPrivileges(false);
  startsAfter("Logger");
  startsAfter("GeneralServer");
}

void IResearchFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
}

void IResearchFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
}

void IResearchFeature::prepare() {
}

void IResearchFeature::start() {
  GeneralServerFeature::HANDLER_FACTORY->addPrefixHandler(
    RestIResearchViewHandler::IRESEARCH_VIEW_PATH,
    RestHandlerCreator<RestIResearchViewHandler>::createNoData
  );
}

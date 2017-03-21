'use strict';
const _ = require('lodash');
const dd = require('dedent');
const fs = require('fs');
const joi = require('joi');
const semver = require('semver');

const actions = require('@arangodb/actions');
const ArangoError = require('@arangodb').ArangoError;
const errors = require('@arangodb').errors;
const jsonml2xml = require('@arangodb/util').jsonml2xml;
const swaggerJson = require('@arangodb/foxx/legacy/swagger').swaggerJson;
const fm = require('@arangodb/foxx/manager');
const fmu = require('@arangodb/foxx/manager-utils');
const createRouter = require('@arangodb/foxx/router');
const reporters = Object.keys(require('@arangodb/mocha').reporters);
const schemas = require('./schemas');

const router = createRouter();
module.context.registerType('multipart/form-data', require('./multipart'));
module.context.use(router);

const LDJSON = 'application/x-ldjson';

const legacyErrors = new Map([
  [errors.ERROR_SERVICE_INVALID_NAME.code, errors.ERROR_SERVICE_SOURCE_NOT_FOUND.code],
  [errors.ERROR_SERVICE_INVALID_MOUNT.code, errors.ERROR_INVALID_MOUNTPOINT.code],
  [errors.ERROR_SERVICE_DOWNLOAD_FAILED.code, errors.ERROR_SERVICE_SOURCE_ERROR.code],
  [errors.ERROR_SERVICE_UPLOAD_FAILED.code, errors.ERROR_SERVICE_SOURCE_ERROR.code]
]);

const serviceToJson = (service) => (
  {
    mount: service.mount,
    path: service.basePath,
    name: service.manifest.name,
    version: service.manifest.version,
    development: service.isDevelopment,
    legacy: service.legacy,
    manifest: service.manifest,
    options: _.pick(service.options, ['configuration', 'dependencies'])
  }
);

function isLegacy (service) {
  const range = service.manifest.engines && service.manifest.engines.arangodb;
  return range ? semver.gtr('3.0.0', range) : false;
}

function writeUploadToTempFile (buffer) {
  const filename = fs.getTempFile('foxx-manager', true);
  fs.writeFileSync(filename, buffer);
  return filename;
}

function prepareServiceRequestBody (req, res, next) {
  if (req.body.source instanceof Buffer) {
    req.body.source = writeUploadToTempFile(req.body.source);
  }
  try {
    if (req.body.dependencies) {
      req.body.dependencies = JSON.parse(req.body.dependencies);
    }
    if (req.body.configuration) {
      req.body.configuration = JSON.parse(req.body.configuration);
    }
  } catch (e) {
    throw new ArangoError({
      errorNum: errors.ERROR_SERVICE_OPTIONS_MALFORMED.code,
      errorMessage: dd`
        ${errors.ERROR_SERVICE_OPTIONS_MALFORMED.message}
        Details: ${e.message}
      `
    }, {cause: e});
  }
  next();
}

router.use((req, res, next) => {
  try {
    next();
  } catch (e) {
    if (e.isArangoError) {
      const errorNum = legacyErrors.get(e.errorNum) || e.errorNum;
      const status = actions.arangoErrorToHttpCode(errorNum);
      res.throw(status, e.errorMessage, {errorNum, cause: e});
    }
    throw e;
  }
});

router.get((req, res) => {
  res.json(
    fmu.getStorage().toArray()
    .map((service) => (
      {
        mount: service.mount,
        name: service.manifest.name,
        version: service.manifest.version,
        provides: service.manifest.provides || {},
        development: service.isDevelopment,
        legacy: isLegacy(service)
      }
    ))
  );
})
.response(200, joi.array().items(schemas.shortInfo).required(), `Array of service descriptions.`)
.summary(`List installed services`)
.description(dd`
  Fetches a list of services installed in the current database.
`);

router.post(prepareServiceRequestBody, (req, res) => {
  const mount = req.queryParams.mount;
  fm.install(req.body.source, mount, _.omit(req.queryParams, ['mount', 'development']));
  if (req.body.configuration) {
    fm.setConfiguration(mount, {configuration: req.body.configuration, replace: true});
  }
  if (req.body.dependencies) {
    fm.setDependencies(mount, {dependencies: req.body.dependencies, replace: true});
  }
  if (req.queryParams.development) {
    fm.development(mount);
  }
  const service = fm.lookupService(mount);
  res.json(serviceToJson(service));
})
.body(schemas.service, ['multipart/form-data', 'application/json'], `Service to be installed.`)
.queryParam('mount', schemas.mount, `Mount path the service should be installed at.`)
.queryParam('development', schemas.flag.default(false), `Enable development mode.`)
.queryParam('setup', schemas.flag.default(true), `Run the service's setup script.`)
.queryParam('legacy', schemas.flag.default(false), `Service should be installed in 2.8 legacy compatibility mode.`)
.response(201, schemas.fullInfo, `Description of the installed service.`)
.summary(`Install new service`)
.description(dd`
  Installs the given new service at the given mount path.

  The service source can be specified as either an absolute local file path,
  a fully qualified URL reachable from the database server,
  or as a binary zip file using multipart form upload.
`);

const instanceRouter = createRouter();
instanceRouter.use((req, res, next) => {
  const mount = req.queryParams.mount;
  try {
    req.service = fm.lookupService(mount);
  } catch (e) {
    res.throw(400, `No service installed at mount path "${mount}".`, e);
  }
  next();
})
.queryParam('mount', schemas.mount, `Mount path of the installed service.`);
router.use(instanceRouter);

const serviceRouter = createRouter();
instanceRouter.use('/service', serviceRouter);

serviceRouter.get((req, res) => {
  res.json(serviceToJson(req.service));
})
.response(200, schemas.fullInfo, `Description of the service.`)
.summary(`Service description`)
.description(dd`
  Fetches detailed information for the service at the given mount path.
`);

serviceRouter.patch(prepareServiceRequestBody, (req, res) => {
  const mount = req.queryParams.mount;
  fm.upgrade(req.body.source, mount, _.omit(req.queryParams, ['mount']));
  if (req.body.configuration) {
    fm.setConfiguration(mount, {configuration: req.body.configuration, replace: false});
  }
  if (req.body.dependencies) {
    fm.setDependencies(mount, {dependencies: req.body.dependencies, replace: false});
  }
  const service = fm.lookupService(mount);
  res.json(serviceToJson(service));
})
.body(schemas.service, ['multipart/form-data', 'application/json'], `Service to be installed.`)
.queryParam('teardown', schemas.flag.default(false), `Run the old service's teardown script.`)
.queryParam('setup', schemas.flag.default(true), `Run the new service's setup script.`)
.queryParam('legacy', schemas.flag.default(false), `Service should be installed in 2.8 legacy compatibility mode.`)
.response(200, schemas.fullInfo, `Description of the new service.`)
.summary(`Upgrade service`)
.description(dd`
  Installs the given new service on top of the service currently installed at the given mount path.
  This is only recommended for switching between different versions of the same service.

  The service source can be specified as either an absolute local file path,
  a fully qualified URL reachable from the database server,
  or as a binary zip file using multipart form upload.
`);

serviceRouter.put(prepareServiceRequestBody, (req, res) => {
  const mount = req.queryParams.mount;
  fm.replace(req.body.source, mount, _.omit(req.queryParams, ['mount']));
  if (req.body.configuration) {
    fm.setConfiguration(mount, {configuration: req.body.configuration, replace: true});
  }
  if (req.body.dependencies) {
    fm.setDependencies(mount, {dependencies: req.body.dependencies, replace: true});
  }
  const service = fm.lookupService(mount);
  res.json(serviceToJson(service));
})
.body(schemas.service, ['multipart/form-data', 'application/json'], `Service to be installed.`)
.queryParam('teardown', schemas.flag.default(true), `Run the old service's teardown script.`)
.queryParam('setup', schemas.flag.default(true), `Run the new service's setup script.`)
.queryParam('legacy', schemas.flag.default(false), `Service should be installed in 2.8 legacy compatibility mode.`)
.response(200, schemas.fullInfo, `Description of the new service.`)
.summary(`Replace service`)
.description(dd`
  Removes the service at the given mount path from the database and file system.
  Then installs the given new service at the same mount path.

  The service source can be specified as either an absolute local file path,
  a fully qualified URL reachable from the database server,
  or as a binary zip file using multipart form upload.
`);

serviceRouter.delete((req, res) => {
  fm.uninstall(
    req.queryParams.mount,
    _.omit(req.queryParams, ['mount'])
  );
  res.status(204);
})
.queryParam('teardown', schemas.flag.default(true), `Run the service's teardown script.`)
.response(204, null, `Empty response.`)
.summary(`Uninstall service`)
.description(dd`
  Removes the service at the given mount path from the database and file system.
`);

const configRouter = createRouter();
instanceRouter.use('/configuration', configRouter)
.response(200, schemas.configs, `Configuration options of the service.`);

configRouter.get((req, res) => {
  res.json(fm.configuration(req.service.mount));
})
.summary(`Get configuration options`)
.description(dd`
  Fetches the current configuration for the service at the given mount path.
`);

configRouter.patch((req, res) => {
  const warnings = fm.setConfiguration(req.service.mount, {
    configuration: req.body,
    replace: false
  });
  const values = fm.configuration(req.service.mount, {simple: true});
  res.json({values, warnings});
})
.body(joi.object().required(), `Object mapping configuration names to values.`)
.summary(`Update configuration options`)
.description(dd`
  Replaces the given service's configuration.
  Any omitted options will be ignored.
`);

configRouter.put((req, res) => {
  const warnings = fm.setConfiguration(req.service.mount, {
    configuration: req.body,
    replace: true
  });
  const values = fm.configuration(req.service.mount, {simple: true});
  res.json({values, warnings});
})
.body(joi.object().required(), `Object mapping configuration names to values.`)
.summary(`Replace configuration options`)
.description(dd`
  Replaces the given service's configuration completely.
  Any omitted options will be reset to their default values or marked as unconfigured.
`);

const depsRouter = createRouter();
instanceRouter.use('/dependencies', depsRouter)
.response(200, schemas.deps, `Dependency options of the service.`);

depsRouter.get((req, res) => {
  res.json(fm.dependencies(req.service.mount));
})
.summary(`Get dependency options`)
.description(dd`
  Fetches the current dependencies for service at the given mount path.
`);

depsRouter.patch((req, res) => {
  const warnings = fm.setDependencies(req.service.mount, {
    dependencies: req.body,
    replace: true
  });
  const values = fm.dependencies(req.service.mount, {simple: true});
  res.json({values, warnings});
})
.body(joi.object().required(), `Object mapping dependency aliases to mount paths.`)
.summary(`Update dependency options`)
.description(dd`
  Replaces the given service's dependencies.
  Any omitted dependencies will be ignored.
`);

depsRouter.put((req, res) => {
  const warnings = fm.setDependencies(req.service.mount, {
    dependencies: req.body,
    replace: true
  });
  const values = fm.dependencies(req.service.mount, {simple: true});
  res.json({values, warnings});
})
.body(joi.object().required(), `Object mapping dependency aliases to mount paths.`)
.summary(`Replace dependency options`)
.description(dd`
  Replaces the given service's dependencies completely.
  Any omitted dependencies will be disabled.
`);

const devRouter = createRouter();
instanceRouter.use('/development', devRouter)
.response(200, schemas.fullInfo, `Description of the service.`);

devRouter.post((req, res) => {
  const service = fm.development(req.service.mount);
  res.json(serviceToJson(service));
})
.summary(`Enable development mode`)
.description(dd`
  Puts the service into development mode.
  The service will be re-installed from the filesystem for each request.
`);

devRouter.delete((req, res) => {
  const service = fm.production(req.service.mount);
  res.json(serviceToJson(service));
})
.summary(`Disable development mode`)
.description(dd`
  Puts the service at the given mount path into production mode.
  Changes to the service's code will no longer be reflected automatically.
`);

const scriptsRouter = createRouter();
instanceRouter.use('/scripts', scriptsRouter);

scriptsRouter.get((req, res) => {
  res.json(req.service.getScripts());
})
.response(200, joi.array().items(joi.object({
  name: joi.string().required().description(`Script name`),
  title: joi.string().required().description(`Human-readable script name`)
}).required()).required(), `List of scripts available on the service.`)
.summary(`List service scripts`)
.description(dd`
  Fetches a list of the scripts defined by the service.
`);

scriptsRouter.post('/:name', (req, res) => {
  const service = req.service;
  const scriptName = req.pathParams.name;
  res.json(fm.runScript(scriptName, service.mount, req.body) || null);
})
.body(joi.any(), `Optional script arguments.`)
.pathParam('name', joi.string().required(), `Name of the script to run`)
.response(200, joi.any().default(null), `Script result if any.`)
.summary(`Run service script`)
.description(dd`
  Runs the given script for the service at the given mount path.
  Returns the exports of the script, if any.
`);

instanceRouter.post('/download', (req, res) => {
  const service = req.service;
  const dir = fs.join(fs.makeAbsolute(service.root), service.path);
  const zipPath = fmu.zipDirectory(dir);
  const name = service.mount.replace(/^\/|\/$/g, '').replace(/\//g, '_');
  res.download(zipPath, `${name}.zip`);
})
.response(200, ['application/zip'], `Zip bundle of the service.`)
.summary(`Download service bundle`)
.description(dd`
  Creates and downloads a zip bundle of the service directory.
`);

instanceRouter.post('/tests', (req, res) => {
  const service = req.service;
  const reporter = req.queryParams.reporter || null;
  const result = fm.runTests(service.mount, {reporter});
  if (reporter === 'stream' && req.accepts(LDJSON, 'json') === LDJSON) {
    res.type(LDJSON);
    for (const row of result) {
      res.write(JSON.stringify(row) + '\r\n');
    }
  } else if (reporter === 'xunit' && req.accepts('xml', 'json') === 'xml') {
    res.type('xml');
    res.write('<?xml version="1.0" encoding="utf-8"?>\n');
    res.write(jsonml2xml(result) + '\n');
  } else if (reporter === 'tap' && req.accepts('text', 'json') === 'text') {
    res.type('text');
    for (const row of result) {
      res.write(row + '\n');
    }
  } else {
    res.json(result);
  }
})
.queryParam('reporter', joi.only(...reporters).optional(), `Test reporter to use`)
.response(200, ['json', LDJSON, 'xml', 'text'], `Test results.`)
.summary(`Run service tests`)
.description(dd`
  Runs the tests for the service at the given mount path and returns the results.
`);

instanceRouter.get('/readme', (req, res) => {
  const service = req.service;
  res.send(fm.readme(service.mount));
})
.response(200, ['text/plain'], `Raw README contents.`)
.summary(`Service README`)
.description(dd`
  Fetches the service's README or README.md file's contents if any.
`);

instanceRouter.get('/swagger', (req, res) => {
  swaggerJson(req, res, {
    mount: req.service.mount
  });
})
.response(200, joi.object(), `Service Swagger description.`)
.summary(`Swagger description`)
.description(dd`
  Fetches the Swagger API description for the service at the given mount path.
`);

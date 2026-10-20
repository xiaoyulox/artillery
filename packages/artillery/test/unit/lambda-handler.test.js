/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// Unit tests for the Lambda worker handler's timeout watchdog
// (lib/platform/aws-lambda/lambda-handler/a9-handler-index.js).
//
// Regression tests for https://github.com/artilleryio/artillery/issues/3782:
// the watchdog interval must be cleared when the handler finishes so it
// cannot be frozen with the execution environment and resumed by a later
// invocation, and a failing watchdog send must never crash the process.
//
// The handler's dependencies (AWS SDK clients, helper modules) are mocked
// by seeding require.cache before the handler module is required.

const { test, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');

const HANDLER_PATH = require.resolve(
  '../../lib/platform/aws-lambda/lambda-handler/a9-handler-index.js'
);
const HANDLER_DIR = path.dirname(HANDLER_PATH);
const SQS_MODULE_PATH = require.resolve('@aws-sdk/client-sqs', {
  paths: [HANDLER_DIR]
});
const S3_MODULE_PATH = require.resolve('@aws-sdk/client-s3', {
  paths: [HANDLER_DIR]
});
const HELPERS_PATH = require.resolve(
  '../../lib/platform/aws-lambda/lambda-handler/a9-handler-helpers.js'
);
const DEPENDENCIES_PATH = require.resolve(
  '../../lib/platform/aws-lambda/lambda-handler/a9-handler-dependencies.js'
);

function seedModule(absolutePath, exports) {
  require.cache[absolutePath] = {
    id: absolutePath,
    filename: absolutePath,
    loaded: true,
    exports
  };
}

class FakeSQSClient {
  send(command) {
    return sqsSendImpl(command.input);
  }
}

class FakeSendMessageCommand {
  constructor(input) {
    this.input = input;
  }
}

class FakeS3Client {
  send() {
    return Promise.resolve({});
  }
}

class FakeHeadObjectCommand {
  constructor(input) {
    this.input = input;
  }
}

const BASE_EVENT = {
  SQS_QUEUE_URL: 'https://sqs.us-east-1.amazonaws.com/123456789/test-queue',
  SQS_REGION: 'us-east-1',
  TEST_RUN_ID: 'test-run-1',
  WORKER_ID: 'worker-1',
  ARTILLERY_ARGS: [],
  BUCKET: 'test-bucket',
  ENV: {},
  WAIT_FOR_GREEN: false
};

function makeContext(remainingMsec) {
  return { getRemainingTimeInMillis: () => remainingMsec };
}

let handler;
let sqsSendImpl;
let runProcessImpl;
let syncTestDataImpl;
let installDepsImpl;
let setIntervalCalls;
let clearIntervalCalls;
let sqsSent;
let consoleErrorCalls;
let origSetInterval;
let origClearInterval;
let origConsoleLog;
let origConsoleError;

beforeEach(() => {
  setIntervalCalls = [];
  clearIntervalCalls = [];
  sqsSent = [];
  consoleErrorCalls = [];

  sqsSendImpl = async (input) => {
    sqsSent.push(input);
    return {};
  };
  runProcessImpl = async () => ({ err: null, code: 0, stdout: '', stderr: '' });
  syncTestDataImpl = async () => {};
  installDepsImpl = async () => {};

  seedModule(SQS_MODULE_PATH, {
    SQSClient: FakeSQSClient,
    SendMessageCommand: FakeSendMessageCommand
  });
  seedModule(S3_MODULE_PATH, {
    S3Client: FakeS3Client,
    HeadObjectCommand: FakeHeadObjectCommand
  });
  seedModule(HELPERS_PATH, {
    runProcess: (...args) => runProcessImpl(...args),
    sleep: async () => {}
  });
  seedModule(DEPENDENCIES_PATH, {
    syncTestData: (...args) => syncTestDataImpl(...args),
    installNpmDependencies: (...args) => installDepsImpl(...args)
  });

  delete require.cache[HANDLER_PATH];
  ({ handler } = require(HANDLER_PATH));

  origSetInterval = global.setInterval;
  origClearInterval = global.clearInterval;
  origConsoleLog = console.log;
  origConsoleError = console.error;

  let nextHandleId = 0;
  global.setInterval = (fn, ms) => {
    const handle = {
      id: ++nextHandleId,
      unref() {
        return this;
      }
    };
    setIntervalCalls.push({ handle, fn, ms });
    return handle;
  };
  global.clearInterval = (handle) => {
    clearIntervalCalls.push(handle);
  };
  console.log = () => {};
  console.error = (...args) => {
    consoleErrorCalls.push(args);
  };
});

afterEach(() => {
  global.setInterval = origSetInterval;
  global.clearInterval = origClearInterval;
  console.log = origConsoleLog;
  console.error = origConsoleError;
  delete require.cache[HANDLER_PATH];
});

test('clears the timeout watchdog when the handler finishes', async () => {
  await handler(BASE_EVENT, makeContext(10 * 60 * 1000));

  assert.equal(setIntervalCalls.length, 1);
  assert.equal(setIntervalCalls[0].ms, 5000);
  assert.deepStrictEqual(clearIntervalCalls, [setIntervalCalls[0].handle]);
});

test('clears the timeout watchdog even when the Artillery run fails', async () => {
  runProcessImpl = async () => {
    throw new Error('artillery failed to start');
  };

  await handler(BASE_EVENT, makeContext(10 * 60 * 1000));

  assert.equal(setIntervalCalls.length, 1);
  assert.deepStrictEqual(clearIntervalCalls, [setIntervalCalls[0].handle]);

  const failureNotices = sqsSent.filter((input) =>
    input.MessageBody.includes('StartupFailure')
  );
  assert.equal(failureNotices.length, 1);
});

test('a failing watchdog send does not cause an unhandled rejection', async () => {
  // Simulate a stale watchdog firing against the previous run's queue,
  // which the CLI has already deleted
  sqsSendImpl = async (input) => {
    sqsSent.push(input);
    if (input.MessageBody.includes('AWSLambdaTimeout')) {
      const err = new Error(
        'The specified queue does not exist for this wsdl version.'
      );
      err.name = 'QueueDoesNotExist';
      throw err;
    }
    return {};
  };

  let unhandledRejection = null;
  const onUnhandledRejection = (reason) => {
    unhandledRejection = reason;
  };
  process.on('unhandledRejection', onUnhandledRejection);

  try {
    await handler(BASE_EVENT, makeContext(1000));
    assert.equal(setIntervalCalls.length, 1);

    const clearsBeforeTrigger = clearIntervalCalls.length;

    // Fire the watchdog callback the way the timer would - the returned
    // promise is deliberately not awaited, so a rejection inside it would
    // surface as an unhandled rejection, exactly as on Lambda
    const watchdogCallback = setIntervalCalls[0].fn;
    watchdogCallback();

    // Let the watchdog's promise chain settle
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.strictEqual(unhandledRejection, null);

    const timeoutWarnings = sqsSent.filter((input) =>
      input.MessageBody.includes('AWSLambdaTimeout')
    );
    assert.equal(timeoutWarnings.length, 1);

    assert.ok(
      consoleErrorCalls.some(
        (args) => args[0] === 'Error in Lambda timeout watchdog:'
      )
    );

    // A failed send must not clear the watchdog, preserving the original
    // retry-on-next-tick behavior for live invocations
    assert.equal(clearIntervalCalls.length, clearsBeforeTrigger);
  } finally {
    process.removeListener('unhandledRejection', onUnhandledRejection);
  }
});

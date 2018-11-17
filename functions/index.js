const functions = require('firebase-functions')
const admin = require('firebase-admin')
const moment = require('moment')
const parser = require('cron-parser')

admin.initializeApp(functions.config().firestore) // Must call before using firebase services
const DB = admin.firestore() // Persistent database handle

DB.settings({
  timestampsInSnapshots: true
})

const COLLECTIONS = Object.freeze({
  JOBS: 'jobs',
  RUNS: 'runs',
  ALERTS: 'alerts',
  MONITORS: 'monitors'
})

const COMPARATOR = Object.freeze({
  LT: '<',
  LTE: '<=',
  EQ: '==',
  NEQ: '!=',
  GT: '>',
  GTE: '>='
})

const ALERT_TYPES = Object.freeze({
  MISSING_RUN: 'MISSING_RUN',
  INVALID_RUN: 'INVALID_RUN',
  ERRORS_IN_RUN: 'ERRORS_IN_RUN'
})

const DEFAULTS = Object.freeze({
  // Fallbacks if job does not define +/- seconds window for valid runs
  MAX_SECONDS_EARLY: 59,
  MAX_SECONDS_LATE: 59,
  // Process runs that began @ (/validate invocation time - CUTOFF_MINUTES) or earlier
  CUTOFF_MINUTES: 1,
  MIN_SECONDS_BETWEEN_VALIDATIONS: 15
})

const endRun = async runData => {
  try {
    if (runData.stdErr !== false) {
      await createAlert(ALERT_TYPES.ERRORS_IN_RUN, {
        runId: runData.runId,
        jobId: runData.jobId
      })
    }
    const runRef = DB.collection(COLLECTIONS.RUNS).doc(runData.runId)
    await DB.runTransaction(async t => {
      const doc = await t.get(runRef)
      if (doc.data().end !== null) {
        throw new Error('That job already ended!')
      }
      const now = new Date()
      const startTime = doc.data().start.toDate()
      const duration = (now - startTime) / 1000 // Runtime in seconds
      const validRun = runData.stdErr === true ? false : null
      await t.update(runRef, {
        durationSeconds: duration,
        end: now,
        reportedEnd: new Date(runData.reportedEnd * 1000),
        stdOut: runData.stdOut,
        stdErr: runData.stdErr,
        validRun: validRun
      })
    })
    return true
  } catch (error) {
    throw Error(error)
  }
}

const updateMonitorLastRuntime = async (monitorId = 'monitor_1') => {
  console.debug(`updateMonitorLastRuntime monitorId: ${monitorId}`)
  const monitorRef = DB.collection(COLLECTIONS.MONITORS).doc(monitorId)
  try {
    await DB.runTransaction(async t => {
      const ref = await t.get(monitorRef)
      t.update(monitorRef, {
        lastRunTimestamp: new Date()
      })
    })
    return 'Success'
  } catch (error) {
    throw Error(error)
  }
}

const shouldMonitorRun = async (monitorId = 'monitor_1') => {
  console.debug(`shouldMonitorRun monitorId: ${monitorId}`)
  const monitorRef = DB.collection(COLLECTIONS.MONITORS).doc(monitorId)
  const docSnapshot = await monitorRef.get()
  console.log(docSnapshot)
  // if (!docSnapshot || docSnapshot.docs.length === 0) {
  //   throw Error(`Could not find monitor with id: ${monitorId}`)
  // } // TODO: halp this is a QuerySnapshot?!
  const monitorData = docSnapshot.docs.map(doc => doc.data())
  const lastRunTime = monitorData.lastRunTimestamp
  if (!lastRunTime) {
    return true
  } else {
    return (
      moment().diff(moment.unix(lastRunTime._seconds), 'seconds') >=
      DEFAULTS.MIN_SECONDS_BETWEEN_VALIDATIONS
    )
  }
}

const updateRun = async (runId, newDataObj) => {
  console.debug(`updateRun: ${runId} ${newDataObj}`)
  const runRef = DB.collection(COLLECTIONS.RUNS).doc(runId)
  try {
    await DB.runTransaction(async t => {
      const ref = await t.get(runRef)
      t.update(runRef, newDataObj)
      const jobId = ref.data().jobId
      if (newDataObj.validRun === false) {
        await createAlert(ALERT_TYPES.INVALID_RUN, {
          runId: runId,
          jobId: jobId,
          text: 'Please fix this invalid run!'
        })
      }
    })
    return 'Success'
  } catch (error) {
    throw Error(error)
  }
}

const updateJob = async (jobId, newDataObj) => {
  console.debug(`updateJob: ${jobId} ${newDataObj}`)
  const jobRef = DB.collection(COLLECTIONS.JOBS).doc(jobId)
  try {
    await DB.runTransaction(async t => {
      const ref = await t.get(jobRef)
      t.update(jobRef, newDataObj)
    })
    return 'Success'
  } catch (error) {
    throw Error(error)
  }
}

/* Alerts */

// Firebase trigger for each new alert used to dispatch messages to users who are
// subscribed to alerts for the given jobId in the organization.
exports.createAlert = functions.firestore
  .document('alerts/{alertId}')
  .onCreate((snap, context) => {
    console.debug(`Alert onCreate Trigger: ${JSON.stringify(data)}`)
    // Can access alert information with snap.data()
    const data = snap.data()
    return snap.ref.update({
      // TODO: alert said subscribed members
      subscribedMembersAlerted: true
    })
  })

const createAlert = async (alertType, alertData) => {
  console.debug(`createAlert: ${alertType} ${JSON.stringify(alertData)}`)
  // Generates an alert of @alertType with @alertData
  if (alertType === null) {
    console.error('You should not create an alert without an alertType!')
  }
  const { jobId, runId, text } = alertData
  const alertId = `alert_${Math.round(Date.now() / 1000)}_${jobId}`
  try {
    await DB.collection(COLLECTIONS.ALERTS).doc(alertId).set({
      alertType: alertType,
      text: text || null,
      jobId: jobId || null,
      runId: runId || null,
      createdAt: new Date(),
      resolvedAt: null,
      resolvedByMember: null,
      subscribedMembersAlerted: false
    })
    return true
  } catch (error) {
    throw Error(error)
  }
}
/* END Alerts */

const getJobsList = async () => {
  try {
    const jobs = await DB.collection(COLLECTIONS.JOBS).get()
    return jobs.docs.map(jobDoc =>
      Object.assign({ id: jobDoc.id }, jobDoc.data())
    )
  } catch (error) {
    throw Error(error)
  }
}

const getRunsForJob = async (jobId, earliestTime, filterFn) => {
  console.debug(`Getting runs for ${jobId} later than ${earliestTime}`)
  /* Retrieve all runs for @jobId that occurred after @optionalStartTimestamp
  If no @optionalStartTimestamp is specified, retrieves all runs for job. */
  let runsRef = DB.collection(COLLECTIONS.RUNS).where(
    'jobId',
    COMPARATOR.EQ,
    jobId
  )
  if (earliestTime) {
    runsRef = runsRef.where('start', COMPARATOR.GTE, earliestTime)
  }
  try {
    const runsForJob = await runsRef.get()
    const data = runsForJob.docs.map(doc =>
      Object.assign({ id: doc.id }, doc.data())
    )
    return filterFn ? data.filter(filterFn) : data
  } catch (error) {
    throw Error(error)
  }
}

const generateTimeSlots = (run, job, M_processRunsBefore) => {
  /* Given a @firstRun time in the list and a @cronSignature,
  generate the expected time slots between that run and @stopPoint, default
  5 mintues ago */
  const cronSignature = job.cronSig
  const maxSecondsEarly = job.allowedSecondsEarly || DEFAULTS.MAX_SECONDS_EARLY
  const maxSecondsLate = job.allowedSecondsLate || DEFAULTS.MAX_SECONDS_LATE
  const runStart = moment.unix(run.start._seconds)
  M_processRunsBefore.clone().add(maxSecondsLate, 'seconds')
  let iteratorStart = runStart.clone().subtract(maxSecondsEarly, 'seconds')
  let iterator = parser.parseExpression(cronSignature, {
    currentDate: iteratorStart.valueOf()
  })
  const iteratorNext = () => iterator.next()
  let time = iteratorNext()._date
  let res = []
  while (time.isBefore(M_processRunsBefore)) {
    const earliestTime = time.clone().subtract(maxSecondsEarly, 'seconds')
    const latestTime = time.clone().add(maxSecondsLate, 'seconds')
    res.push({
      time: time,
      isTaken: false,
      earliest: earliestTime,
      latest: latestTime
    })
    time = iteratorNext()._date
  }
  return res
}

const validateTimesForJob = (timeSlots, job, M_processRunsBefore) => {
  /*
  Returns an object containing an object with arrays of two things:
  runs with statuses (valid / invalid)
  times to generate alerts for (missing runs) */
  const runList = job.runList
  const maxSecondsAllowedLate =
    job.allowedSecondsLate || DEFAULTS.MAX_SECONDS_LATE
  const latestTimeToConsider = M_processRunsBefore.clone().add(
    maxSecondsAllowedLate,
    'seconds'
  )
  let runsWithValidity = [] // array of {runId: the_run_id, validRun: true/false}
  for (var i = 0; i < runList.length; i++) {
    const run = runList[i]
    let matchFound = false
    let thisRunTime = moment.unix(run.start._seconds)
    if (thisRunTime.isAfter(latestTimeToConsider)) {
      console.debug(`Skipping run at ${thisRunTime}`)
      continue
    }
    for (var j = 0; j < timeSlots.length; j++) {
      const slot = timeSlots[j]
      if (slot.isTaken) {
        continue
      }
      if (thisRunTime.isBetween(slot.earliest, slot.latest)) {
        slot.isTaken = true
        matchFound = true
        runsWithValidity.push({
          run: run,
          validRun: true
        })
      }
    }
    if (matchFound === false) {
      runsWithValidity.push({
        run: run,
        validRun: false
      })
    }
  }
  // Create flat list of time slots that are not taken yet and remove the isTaken flag
  let orphanTimeSlots = timeSlots
    .filter(slot => !slot.isTaken)
    .map(slot => slot.time)
  return {
    orphanTimeSlots: orphanTimeSlots,
    runsWithValidity: runsWithValidity
  }
}

const getJobData = async M_processRunsBefore => {
  const jobList = await getJobsList()
  const runPromises = jobList.map(async job => {
    const earliestRunTime = job.validatedUntilTimestamp || null
    const filterFn = run =>
      run.start && moment.unix(run.start._seconds).isBefore(M_processRunsBefore)
    let runList = await getRunsForJob(job.id, earliestRunTime, filterFn)
    runList = runList.map(run => {
      return {
        runId: run.id,
        start: run.start,
        end: run.end
      }
    })
    return {
      jobId: job.id,
      cronSig: job.cronSig,
      runList: runList,
      jobData: job.jobData
    }
  })
  const jobData = await Promise.all(runPromises)
  return jobData
}

const getValidationResults = (jobData, M_processRunsBefore) => {
  console.debug(`getValidationResults: ${JSON.stringify(jobData)}`)
  // Will not consider any runs after M_processRunsBefore on this validation run.
  return jobData.filter(jobData => jobData.runList.length > 0).map(job => {
    let firstRun = job.runList[0]
    let jobTimeSlots = generateTimeSlots(firstRun, job, M_processRunsBefore)
    return {
      job: job.jobId,
      results: validateTimesForJob(jobTimeSlots, job, M_processRunsBefore),
      processedAt: M_processRunsBefore.toDate()
    }
  })
}

const processValidationResults = async (
  validationResults,
  M_processRunsBefore
) => {
  // Updates runs and jobs, generates alerts for failures
  console.debug(
    `processValidationResults: ${JSON.stringify(validationResults)}`
  )
  const processBeforeDate = M_processRunsBefore.clone().toDate()
  let promisesToAwait = []
  validationResults.forEach(jobValidationResult => {
    const { job, results, processedAt } = jobValidationResult
    const { orphanTimeSlots, runsWithValidity } = results
    promisesToAwait.push(
      ...runsWithValidity.map(run =>
        updateRun(run.run.runId, {
          validRun: run.validRun
        })
      )
    )
    /// If we didn't look at any runs for this job, don't update validatedUntilTimestamp
    if (orphanTimeSlots.length !== 0 || runsWithValidity.length !== 0) {
      promisesToAwait.push(
        updateJob(job, {
          validatedUntilTimestamp: processBeforeDate
        })
      )
    }
    promisesToAwait.push(
      ...orphanTimeSlots.map(orphan =>
        createAlert(ALERT_TYPES.MISSING_RUN, {
          jobId: job,
          text: orphan.toDate()
        })
      )
    )
  })
  try {
    await Promise.all(promisesToAwait)
    return `Processed ${promisesToAwait.length} updates successfully.`
  } catch (error) {
    throw Error(error)
  }
}

const checkRuns = async (toPresent = false) => {
  console.debug(`checkRuns toPresent: ${toPresent}`)
  try {
    const shouldRun = await shouldMonitorRun()
    console.log(`shouldRun: ${shouldRun}`)
    const M_processRunsBefore = toPresent
      ? moment()
      : moment().subtract(DEFAULTS.CUTOFF_MINUTES, 'minutes')
    const jobData = await getJobData(M_processRunsBefore)
    const validationResults = await getValidationResults(
      jobData,
      M_processRunsBefore
    )
    const processTheResults = await processValidationResults(
      validationResults,
      M_processRunsBefore
    )
    await updateMonitorLastRuntime()
    return
  } catch (error) {
    console.error(error)
  }
}

// Firebase trigger for each new alert used to dispatch messages to users who are
// subscribed to alerts for the given jobId in the organization.
exports.validate = functions.firestore
  .document('runs/{runId}')
  .onCreate(async (snap, context) => {
    console.debug(`validate onCreate trigger ${new Date()}`)
    return await checkRuns()
  })

exports.stop = functions.https.onRequest(async (req, res) => {
  const { runId, jobId, reportedEnd, stdOut, stdErr, APIKEY } = req.body
  console.debug(`stop ${JSON.stringify(req.body)}`)
  if (APIKEY !== process.env.SECRET) {
    return res.sendStatus(401)
  }
  const failInvalid = msg =>
    res.status(500).send(`Please provide a valid ${msg}`)

  if (!runId || !runId.length) {
    return failInvalid('runId')
  } else if (!jobId || !jobId.length) {
    return failInvalid('jobId')
  } else if (!reportedEnd) {
    return failInvalid('reportedEnd')
  } else if (stdOut === null) {
    return failInvalid('stdOut')
  } else if (stdErr === null) {
    return failInvalid('stdErr')
  }

  const runData = {
    jobId: jobId,
    runId: runId,
    reportedEnd: reportedEnd,
    stdOut: stdOut !== 'False',
    stdErr: stdErr !== 'False'
  }
  try {
    const update = await endRun(runData)
    checkRuns(true)
    return res.status(200).send(update)
  } catch (error) {
    console.error(error)
    return res.status(500).send(`Error: ${error.message}`)
  }
})

exports.go = functions.https.onRequest(async (req, res) => {
  console.debug(`go ${JSON.stringify(req.body)}`)
  const { runId, jobId, reportedStart, APIKEY } = req.body
  if (APIKEY !== process.env.SECRET) {
    return res.sendStatus(401)
  }
  const failInvalid = msg =>
    res.status(500).send(`Please provide a valid ${msg}`)

  if (!runId || !runId.length) {
    return failInvalid('runId')
  } else if (!jobId || !jobId.length) {
    return failInvalid('jobId')
  } else if (!reportedStart) {
    return failInvalid('reportedStart')
  }

  try {
    const update = await DB.collection(COLLECTIONS.RUNS).doc(runId).set({
      jobId: jobId,
      start: new Date(),
      end: null,
      reportedStart: new Date(reportedStart * 1000),
      reportedEnd: null,
      validRun: null
    })
    return res.sendStatus(200)
  } catch (error) {
    console.error(error)
    return res.status(500).send(error)
  }
})

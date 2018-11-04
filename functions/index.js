const functions = require('firebase-functions')
const admin = require('firebase-admin')
const url = require('url')
const moment = require('moment')
const parser = require('cron-parser')

admin.initializeApp(functions.config().firestore)
admin.firestore().settings({
  timestampsInSnapshots: true
})

const COLLECTIONS = Object.freeze({
  JOBS: 'jobs',
  RUNS: 'runs',
  ALERTS: 'alerts'
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

// TODO: endRun just wraps an updateRun call with end-specific data.
const endRun = async runData => {
  const runRef = admin
    .firestore()
    .collection(COLLECTIONS.RUNS)
    .doc(runData.runId)
  if (runData.stdErr !== false) {
    await createAlert(ALERT_TYPES.ERRORS_IN_RUN, {
      runId: runData.runId,
      jobId: runData.jobId
    })
  }
  return new Promise((resolve, reject) => {
    admin.firestore().runTransaction(t => {
      return t
        .get(runRef)
        .then(doc => {
          if (doc.data().end !== null) {
            return reject(new Error('That job already ended!'))
          }
          const now = new Date()
          const startTime = doc.data().start.toDate()
          const duration = (now - startTime) / 1000 // Time difference in seconds
          const validRun = runData.stdErr === true ? false : null
          t.update(runRef, {
            durationSeconds: duration,
            end: now,
            reportedEnd: new Date(runData.reportedEnd * 1000),
            stdOut: runData.stdOut,
            stdErr: runData.stdErr,
            validRun: validRun
          })
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
}

const updateRun = async (runId, newDataObj) => {
  console.log('Updating run', runId)
  const runRef = admin.firestore().collection(COLLECTIONS.RUNS).doc(runId)
  try {
    await admin.firestore().runTransaction(async t => {
      const ref = await t.get(runRef)
      t.update(runRef, newDataObj)
      console.log('the data', ref.data())
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

const updateJob = (jobId, newDataObj) => {
  console.log('Updating job', jobId)
  const jobRef = admin.firestore().collection(COLLECTIONS.JOBS).doc(jobId)
  return new Promise((resolve, reject) => {
    return admin.firestore().runTransaction(t => {
      return t
        .get(jobRef)
        .then(doc => {
          // TODO: rollback if doc is messed up
          // right now we do nothing with it heh
          t.update(jobRef, newDataObj)
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
}

/* Alerts */

// Firebase trigger on creation of each new alert
// Used to dispatch messages to users who are subscribed to
// alerts for the given jobId in the organization.
exports.createAlert = functions.firestore
  .document('alerts/{alertId}')
  .onCreate((snap, context) => {
    // Can access alert information with snap.data()
    const data = snap.data()
    console.log('TODO: Send this message to people who care.')
    console.log('Alert!', JSON.stringify(data))
    snap.ref.update({
      subscribedMembersAlerted: true
    })
  })

const createAlert = async (alertType, alertData) => {
  // Generates an alert of @alertType with @alertData
  if (alertType === null) {
    console.error('You should not create an alert without an alertType!')
  }
  const { jobId, runId, text } = alertData
  const alertId = `alert_${Math.round(Date.now() / 1000)}_${jobId}`
  return admin.firestore().collection(COLLECTIONS.ALERTS).doc(alertId).set({
    alertType: alertType,
    text: text || null,
    jobId: jobId || null,
    runId: runId || null,
    createdAt: new Date(),
    resolvedAt: null,
    resolvedByMember: null,
    subscribedMembersAlerted: false
  })
}
/* END Alerts */

const getJobsList = async () => {
  const jobsRef = admin.firestore().collection(COLLECTIONS.JOBS)
  let jobs = await jobsRef.get()
  return jobs.docs.map(jobDoc => {
    let data = jobDoc.data()
    return {
      jobId: jobDoc.id,
      cronSig: data.cronSig,
      validatedUntilTimestamp: data.validatedUntilTimestamp || null
    }
  })
}

const getRunsForJob = async (jobId, optionalStartTimestamp = null) => {
  console.log(`Getting runs for ${jobId} ${optionalStartTimestamp}`)
  /* Retrieve all runs for @jobId that occurred after @optionalStartTimestamp
  If no @optionalStartTimestamp is specified, retrieves all runs for job. */
  return new Promise((resolve, reject) => {
    let runsRef = admin
      .firestore()
      .collection(COLLECTIONS.RUNS)
      .where('jobId', COMPARATOR.EQ, jobId)
    if (optionalStartTimestamp !== null) {
      runsRef = runsRef.where('start', COMPARATOR.GTE, optionalStartTimestamp)
    }
    runsRef
      .get()
      .then(querySnapshot => {
        return !querySnapshot
          ? reject(new Error('Run obtaining query failed to run.'))
          : resolve(querySnapshot.docs)
      })
      .catch(err => reject(new Error(err)))
  })
}

const generateTimeSlots = (
  run,
  cronSignature,
  stopPoint = moment().subtract(5, 'minutes')
) => {
  /* Given a @firstRun time in the list and a @cronSignature,
  generate the expected time slots between that run and @stopPoint, default
  5 mintues ago */
  const runStart = moment.unix(run.start._seconds)
  let iteratorStart = runStart.clone().subtract(59, 'seconds')
  let iterator = parser.parseExpression(cronSignature, {
    currentDate: iteratorStart.valueOf()
  })
  const resetIterator = currentTime => {
    iterator = parser.parseExpression(cronSignature, {
      currentDate: currentTime.clone().valueOf()
    })
  }
  const iteratorNext = () => iterator.next()._date
  let time = iteratorNext()
  let res = []
  while (time < stopPoint) {
    // TODO: make sure this won't explode
    res.push({ time: time, isTaken: false })
    time = iteratorNext()
  }
  return res
}

const validateTimesForJobs = (timeSlots, runList) => {
  /*
  Returns an object containing an object with arrays of two things:
  runs with statuses (valid / invalid)
  times to generate alerts for (missing runs)
  {
    runs: [{runId: "iDofTheRun", validRun: True/false }]
    times: [times, that, are, missing, runs]
  }

    A run is valid if it can be attributed to any time
    within the expected run times list within THRESHOLD

    A run is invalid if it cannot be attributed to any
    time within the expected run times list within THRESHOLD

    A run is MISSING if the expected times list does not have an attributed
    run for all runs in the interval.

    A run is marked INVALID if e.g., it could have been attributed
    to a time within the expected run times list, but another
    run got attributed prior (e..g, )
  [
    moment("2018-11-03T10:30:00.000"),
    moment("2018-11-03T10:35:00.000"),
    moment("2018-11-03T10:40:00.000"),
    moment("2018-11-03T10:45:00.000"),
  ]
  [
    { runId: 'auto_freegeo_1541259003',
      start: { _seconds: 1541259003, _nanoseconds: 918000000 },
      end: { _seconds: 1541259004, _nanoseconds: 569000000 },
      expectedTime: moment("2018-11-03T10:30:03.000") },
    { runId: 'auto_freegeo_1541259902',
      start: { _seconds: 1541259902, _nanoseconds: 753000000 },
      end: { _seconds: 1541259903, _nanoseconds: 249000000 },
      expectedTime: moment("2018-11-03T10:45:02.000") },
  ]
  */
  let results = []
  let run = runList[0]
  let times = runList.map(run =>
    Object.assign({}, run, {
      expectedTime: moment.unix(run.start._seconds)
    })
  )
  const latestTimeToConsider = moment().subtract(5, 'minutes')
  let runsWithValidity = [] // array of {runId: the_run_id, validRun: true/false}
  for (var i = 0; i < runList.length; i++) {
    let matchFound = false
    let run = runList[i]
    let time = moment.unix(run.start._seconds)
    if (time >= latestTimeToConsider) {
      // TODO: fix kludge -- SKIP!
      // TODO: should also skip jobs with endTime === null?
      continue
    }
    let possibleTimes = timeSlots.filter(e => !e.isTaken)
    const maxTimeBefore = time.clone()
    const maxTimeAfter = time.clone()
    // TODO: right now +/- 59 seconds (so roughly a ~ 2.99 minute window)
    maxTimeBefore.subtract(59, 'seconds')
    maxTimeAfter.add(59, 'seconds')
    for (var j = 0; j < timeSlots.length; j++) {
      if (timeSlots[j].isTaken === true) {
        continue
      }
      if (timeSlots[j].time.isBetween(maxTimeBefore, maxTimeAfter) === true) {
        timeSlots[j].isTaken = true
        matchFound = true
        runsWithValidity.push({
          run: runList[i],
          validRun: true
        })
      }
    }
    if (matchFound === false) {
      runsWithValidity.push({
        run: runList[i],
        validRun: false
      })
    }
  }
  // Create flat list of time slots that are not taken yet and remove the isTaken flag
  let orphanTimeSlots = timeSlots
    .filter(slot => slot.isTaken === false)
    .map(slot => slot.time)
  return {
    orphanTimeSlots: orphanTimeSlots,
    runsWithValidity: runsWithValidity
  }
}

const getJobData = async () => {
  const jobList = await getJobsList()
  const runPromises = jobList.map(async job => {
    let runList = await getRunsForJob(job.jobId, job.validatedUntilTimestamp)
    runList = runList.map(run => {
      let runData = run.data()
      return {
        runId: run.id,
        start: runData.start,
        end: runData.end
      }
    })
    return {
      jobId: job.jobId,
      cronSig: job.cronSig,
      runList: runList
    }
  })
  const jobData = await Promise.all(runPromises)
  return jobData
}

const getValidationResults = jobData => {
  return jobData.filter(jobData => jobData.runList.length > 0).map(job => {
    let firstRun = job.runList[0]
    let jobTimeSlots = generateTimeSlots(firstRun, job.cronSig)
    return {
      job: job.jobId,
      results: validateTimesForJobs(jobTimeSlots, job.runList)
    }
  })
}

const processValidationResults = async validationResults => {
  console.log(validationResults)
  // Updates runs and jobs, generates alerts for failures
  let promisesToAwait = []
  const lastValidatedTime = admin.firestore.Timestamp.fromDate(
    moment().subtract(5, 'minutes').toDate()
  )
  validationResults.forEach(jobValidationResult => {
    const { job, results } = jobValidationResult
    const { orphanTimeSlots, runsWithValidity } = results
    promisesToAwait.push(
      ...runsWithValidity.map(run =>
        updateRun(run.run.runId, {
          validRun: run.validRun
        })
      )
    )
    /* If we didn't look at any runs for this job,
    don't update validatedUntil. */
    if (orphanTimeSlots.length !== 0 || runsWithValidity.length !== 0) {
      promisesToAwait.push(
        updateJob(job, {
          validatedUntilTimestamp: lastValidatedTime
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

exports.validate = functions.https.onRequest((req, res) => {
  const { APIKEY } = req.body
  if (APIKEY !== process.env.SECRET) {
    return res.sendStatus(401)
  }
  return getJobData()
    .then(jobData => getValidationResults(jobData))
    .then(validationResults => processValidationResults(validationResults))
    .then(yay => res.status(200).send(yay))
    .catch(err => res.status(500).send(err))
})

exports.stop = functions.https.onRequest((req, res) => {
  const { runId, jobId, reportedEnd, stdOut, stdErr, APIKEY } = req.body
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
  // TODO: catch this error clientside and mailgun us or sumthin
  return endRun(runData)
    .then(yay => res.status(200).send(yay))
    .catch(err => res.status(500).send(`Error: ${err.message}`))
})

exports.go = functions.https.onRequest((req, res) => {
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

  return admin
    .firestore()
    .collection(COLLECTIONS.RUNS)
    .doc(runId)
    .set({
      jobId: jobId,
      start: new Date(),
      end: null,
      reportedStart: new Date(reportedStart * 1000),
      reportedEnd: null,
      validRun: null
    })
    .then(snapshot => res.sendStatus(200))
    .catch(err => res.status(500).send(err))
})

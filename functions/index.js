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
  INVALID_RUN: 'INVALID_RUN'
})

const endRun = runData => {
  const runRef = admin
    .firestore()
    .collection(COLLECTIONS.RUNS)
    .doc(runData.runId)
  return new Promise((resolve, reject) => {
    admin.firestore().runTransaction(t => {
      return t
        .get(runRef)
        .then(doc => {
          /* TODO: rollback if doc is messed up
             right now we do nothing with it heh
          */
          if (doc.data().end !== null) {
            return reject(new Error('That job already ended!'))
          }
          const now = new Date()
          const startTime = doc.data().start.toDate()
          const duration = (now - startTime) / 1000 // Time difference in seconds
          // TODO: if stdErr not empty -- run is invalid, so send an alert.
          // TODO: difference between
          // const isInvalid = Boolean(runData.validRun) === true;
          t.update(runRef, {
            durationSeconds: duration,
            end: now,
            reportedEnd: new Date(runData.reportedEnd * 1000),
            stdOut: runData.stdOut,
            stdErr: runData.stdErr
          })
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
}

const updateRun = (runId, newDataObj) => {
  console.log('Updating run', runId)
  const runRef = admin.firestore().collection(COLLECTIONS.RUNS).doc(runId)
  return new Promise((resolve, reject) => {
    admin.firestore().runTransaction(t => {
      return t
        .get(runRef)
        .then(doc => {
          console.info(`Updating ${runId}. Valid? ${newDataObj.validRun}`)
          // TODO: rollback if doc is messed up
          // right now we do nothing with it heh
          t.update(runRef, newDataObj)
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
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

const createAlert = async (alertType, alertInfo, jobId) => {
  // Generates an alert of @alertType with @alertInfo and optional @jobId
  // const alertId = Math.round(Date.now() / 1000)
  // admin.firestore().collection(COLLECTIONS.RUNS).doc(runId).set({})
  console.log(`Oh noes! ${JSON.stringify(alertType)}`)
  // TODO: implement
  return true
}

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
  const runEnd = moment.unix(run.end._seconds)
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
  do {
    // TODO: make sure this won't explode
    res.push({ time: time, isTaken: false })
    time = iteratorNext()
  } while (time < stopPoint)
  return res
}

const validateTimesForJobs = (timeSlots, runList) => {
  /*
  Returns an object containing an object with arrays of two things:
  runs with statuses (valid / invalid)
  times to generate alerts for (missing runs)
  {
    runs: [{runId: "iDofTheRun", isValid: True/false }]
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
  let runsWithValidity = [] // array of {runId: the_run_id, isValid: true/false}
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
          isValid: true
        })
      }
    }
    if (matchFound === false) {
      runsWithValidity.push({
        run: runList[i],
        isValid: false
      })
    }
  }
  let orphanTimeSlots = timeSlots.filter(slot => slot.isTaken === false)
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

/*
  For each unvalidated run, check it and mark it as checked
  For each invalid or missing, create alert
  When done with job, update job validatedUntilTimestamp to the most recent
  run that was checked
*/
exports.validate = functions.https.onRequest((req, res) => {
  const lastValidatedTime = admin.firestore.Timestamp.fromDate(
    moment().subtract(5, 'minutes').toDate()
  )
  getJobData()
    .then(jobData => {
      let validationResults = jobData
        .filter(jobData => jobData.runList.length > 0)
        .map(job => {
          let firstRun = job.runList[0]
          let jobTimeSlots = generateTimeSlots(firstRun, job.cronSig)
          return {
            job: job.jobId,
            results: validateTimesForJobs(jobTimeSlots, job.runList)
          }
        })
      return validationResults.forEach(jobValidationResult => {
        const { job, results } = jobValidationResult
        const { orphanTimeSlots, runsWithValidity } = results
        let runUpdates = runsWithValidity.map(run => {
          return updateRun(run.run.runId, {
            validRun: run.isValid
          })
        })
        // return Promise.all(runUpdates).then(() => {
        //   return res.status(200).send(JSON.stringify(validationResults))
        // })
        // orphanTimeSlots.forEach(orphan => {
        //   createAlert(orphan)
        // })
        // const validatedUntil = runsWithValidity.slice(-1)[0].run.start
        // console.log(validatedUntil)
        // let time = new Date(validatedUntil._seconds)
        // console.log(time)
        updateJob(job, {
          validatedUntilTimestamp: lastValidatedTime
        })
      })
    })
    .catch(err => res.status(500).send(JSON.stringify(err)))
})

exports.stop = functions.https.onRequest((req, res) => {
  const { runId, reportedEnd, stdOut, stdErr } = req.body
  const failInvalid = msg =>
    res.status(500).send(`Please provide a valid ${msg}`)

  if (!runId || !runId.length) {
    return failInvalid('runId')
  } else if (!reportedEnd) {
    return failInvalid('reportedEnd')
  } else if (stdOut === null) {
    return failInvalid('stdOut')
  } else if (stdErr === null) {
    return failInvalid('stdErr')
  }

  const runData = {
    runId: runId,
    reportedEnd: reportedEnd,
    stdOut: stdOut !== 'False',
    stdErr: stdErr !== 'False'
  }
  // TODO: catch this error clientside and mailgun us or sumthin
  return endRun(runData)
    .then(yay => res.status(200).send(yay))
    .catch(err => res.status(200).send(`Error: ${err.message}`))
})

exports.go = functions.https.onRequest((req, res) => {
  const { runId, jobId, reportedStart } = req.body
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
    .catch(err => res.status(500).send(JSON.stringify(err)))
})

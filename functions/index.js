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
  RUNS: 'runs'
})

const COMPARATOR = Object.freeze({
  LT: '<',
  LTE: '<=',
  EQ: '==',
  NEQ: '!=',
  GT: '>',
  GTE: '>='
})

const getPreviousJob = (jobId, numJobsAgo) => {
  // gets the job entry numJobsAgo for job with jobId
  return new Promise((resolve, reject) => {
    if (!jobId.length || !numJobsAgo || !Number.isInteger(numJobsAgo)) {
      return reject(
        new Error('Provide a valid job name and integral i-th job to retrieve.')
      )
    }
    return admin
      .firestore()
      .collection(COLLECTIONS.RUNS)
      .where('job', COMPARATOR.EQ, jobId)
      .orderBy('start', 'desc')
      .limit(numJobsAgo)
      .get()
      .then(querySnapshot => {
        const allGood =
          querySnapshot &&
          !querySnapshot.empty &&
          querySnapshot.docs.length >= numJobsAgo
        return allGood === true
          ? resolve(querySnapshot.docs[numJobsAgo - 1])
          : resolve
      })
      .catch(err => reject(new Error(err)))
  })
}

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

const getCronSignature = async jobId => {
  return new Promise((resolve, reject) => {
    admin
      .firestore()
      .collection(COLLECTIONS.JOBS)
      .where('id', COMPARATOR.EQ, jobId)
      .get()
      .then(querySnapshot => {
        return !querySnapshot || !querySnapshot.docs[0]
          ? reject(
              new Error(
                `No job named ${jobId} exists in the jobs collection in the database.
                Perhaps you just created it?
                Cannot validate runs for this job until the job name and cron signature are added.
                Data for the run will still be recorded.`
              )
            )
          : resolve(querySnapshot.docs[0].data().cronSig)
      })
      .catch(err => reject(new Error(err)))
  })
}

const getRunsRequiringStartValidation = async () => {
  return new Promise((resolve, reject) => {
    admin
      .firestore()
      .collection(COLLECTIONS.RUNS)
      .where('wasStartValidated', COMPARATOR.EQ, false)
      .get()
      .then(querySnapshot => {
        return !querySnapshot
          ? reject(new Error('Start time query failed to run.'))
          : resolve(querySnapshot.docs)
      })
      .catch(err => reject(new Error(err)))
  })
}

const updateRun = (run, newDataObj) => {
  console.info(
    `Updating ${run.data().job} with start ${run.data().start}. Valid? ${newDataObj.isValidStart}`
  )
  return new Promise((resolve, reject) => {
    return admin.firestore().runTransaction(t => {
      return t
        .get(run.ref)
        .then(doc => {
          // TODO: rollback if doc is messed up
          // right now we do nothing with it heh
          t.update(run.ref, newDataObj)
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
}

const validateRunStarts = async () => {
  const runList = await getRunsRequiringStartValidation()
  const result = await Promise.all(
    runList.map(async run => {
      // TODO: memoize the cron signatures or store on the
      // run so we don't have to make a ton of requests here
      const jobId = run.data().job
      try {
        const cronSignature = await getCronSignature(jobId)
        const isValid = validateStartTimeForRun(run, cronSignature)
        await updateRun(run, {
          isValidStart: isValid,
          wasStartValidated: true
        })
      } catch (e) {
        console.error(e)
      }
    })
  )
}

const validateStartTimeForRun = (run, cronSignature) => {
  // TODO: allow custom thresholds (ms before ms after)
  const thisRunData = run.data()
  const runStart = moment.utc(thisRunData.start)
  const initial = parser.parseExpression(cronSignature)
  const A = initial.next()
  const B = initial.prev()
  const step = A._date.diff(B._date, 'milliseconds')
  runStart.add(step - 30, 'seconds')
  const iterati = parser.parseExpression(cronSignature, {
    currentDate: runStart.valueOf()
  })
  const getPrev = () => iterati.prev()._date.utc()
  let nextBack = getPrev()
  // No more than 30 seconds early, no more than 59 seconds late
  const maxTimeBefore = nextBack.clone().utc(true)
  const maxTimeAfter = nextBack.clone().utc(true)
  maxTimeBefore.subtract(30, 'seconds')
  maxTimeAfter.add(59, 'seconds')
  return runStart.isBetween(maxTimeBefore, maxTimeAfter) === true
}

exports.error = functions.https.onRequest((req, res) => {
  getPreviousJob(req.query.job, 1)
    .then(job => endJobDidFail(job, true))
    .then(yay => res.status(200).send(yay))
    .catch(err => res.status(500).send(`Error: ${err.message}`))
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
  return endRun(runData)
    .then(yay => res.status(200).send(yay))
    .catch(err => res.status(200).send(`Error: ${err.message}`))
  /* TODO: insert a trigger to only validate
  previous jobs if we haven't tried in the last N minutes
  */
  // validateRunStarts()
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

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

const endJobDidFail = (job, failed = false) => {
  return new Promise((resolve, reject) => {
    admin.firestore().runTransaction(t => {
      return t
        .get(job.ref)
        .then(doc => {
          // TODO: rollback if doc is messed up
          // right now we do nothing with it heh
          const endTime = Date.now()
          const startTime = doc.data().start
          const duration = endTime - startTime
          t.update(job.ref, {
            duration: duration,
            end: Date.now(),
            error: failed
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
        const gameOver = !querySnapshot || querySnapshot.empty
        return gameOver === true
          ? reject(new Error('Could not find this run!'))
          : resolve(querySnapshot.docs[0].data().cronSig)
      })
      .catch(err => reject(new Error(err)))
  })
}

const validatePreviousRunForJob = job => {
  return new Promise((resolve, reject) => {
    const jobId = job.data().job
    getCronSignature(jobId)
      .then(signature => {
        const parsedCron = parser.parseExpression(signature)
        const prevRunShouldBe = parsedCron.prev()._date.utc()
        const jobStartDate = moment(job.start)
        const isError = prevRunShouldBe.isSame(jobStartDate, 'day') !== true
        return admin.firestore().runTransaction(t => {
          return t
            .get(job.ref)
            .then(doc => {
              // TODO: rollback if doc is messed up
              // right now we do nothing with it heh
              t.update(job.ref, {
                error: isError,
                wasStartValidated: true,
                isValidStart: isValid
              })
              return resolve('Job updated successfully.')
            })
            .catch(err => reject(new Error(err)))
        })
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
      const cronSignature = await getCronSignature(jobId)
      const isValid = validateStartTimeForRun(run, cronSignature)
      await updateRun(run, {
        isValidStart: isValid,
        wasStartValidated: true
      })
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
  runStart.add(step + 500, 'milliseconds')
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
    .then(yay => res.send(200, yay))
    .catch(err => res.send(500, `Error: ${err.message}`))
})

exports.stop = functions.https.onRequest((req, res) => {
  getPreviousJob(req.query.job, 1)
    .then(job => endJobDidFail(job, false))
    .then(yay => res.send(200, yay))
    .catch(err => res.send(500, `Error: ${err.message}`))
  // TODO: insert a trigger to only validate
  // previous jobs if we haven't tried in the last N minutes
  validateRunStarts()
})

exports.go = functions.https.onRequest((req, res) => {
  const jobId = req.query.job
  if (!jobId || !jobId.length) {
    return res.send(500, 'Please provide a valid job name.')
  }
  return admin
    .firestore()
    .collection(COLLECTIONS.RUNS)
    .add({
      job: jobId,
      start: Date.now(),
      end: null,
      wasStartValidated: false,
      isValidStart: null
    })
    .then(snapshot => res.send(200))
    .catch(err => res.send(500, JSON.stringify(err)))
})

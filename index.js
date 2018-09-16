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
    admin
      .firestore()
      .collection(COLLECTIONS.RUNS)
      .where('job', COMPARATOR.EQ, jobId)
      .orderBy('start', 'desc')
      .limit(numJobsAgo)
      .get()
      .then(querySnapshot => {
        const gameOver =
          !querySnapshot ||
          querySnapshot.empty ||
          querySnapshot.docs.length < numJobsAgo
        return gameOver === true
          ? reject(new Error('Could not find this run!'))
          : resolve(querySnapshot.docs[numJobsAgo - 1])
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
            error: failed,
            wasConfirmed: false
          })
          return resolve('Job updated successfully.')
        })
        .catch(err => reject(new Error(err)))
    })
  })
}

const getCronSignature = jobId => {
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
                wasConfirmed: true
              })
              return resolve('Job updated successfully.')
            })
            .catch(err => reject(new Error(err)))
        })
      })
      .catch(err => reject(new Error(err)))
  })
}

exports.error = functions.https.onRequest((req, res) => {
  getPreviousJob(req.query.job, 1)
    .then(job => endJobDidFail(job, true))
    .then(yay => res.send(200, yay))
    .catch(err => res.send(500, `Error: ${err.message}`))
})

exports.stop = functions.https.onRequest((req, res) => {
  // When I stop a job, validate it started on time
  let endJob = getPreviousJob(req.query.job, 1).then(job =>
    endJobDidFail(job, false)
  )
  let validateLast = getPreviousJob(req.query.job, 1).then(job =>
    validatePreviousRunForJob(job)
  )
  Promise.all([endJob, validateLast])
    .then(yay => res.send(200, yay))
    .catch(err => res.send(500, `Error: ${err.message}`))
})

exports.go = functions.https.onRequest((req, res) => {
  const jobId = req.query.job
  if (!jobId || !jobId.length) {
    return res.send(500, 'Please provide a valid job name.')
  }
  // We want to ensure that if there is a previous run for this job
  // that it has an end time. If not, we don't want to start.
  getPreviousJob(req.query.job, 1)
    .then(job => {
      const doc = job.data()
      if (doc.data().end === null) {
        return res.send(500, 'start before we ended this job????')
      }
      return admin
        .firestore()
        .collection(COLLECTIONS.RUNS)
        .add({
          job: jobId,
          start: Date.now(),
          end: null
        })
        .then(snapshot => res.send(200))
        .catch(err => res.send(500, JSON.stringify(err)))
    })
    .catch(err => res.send(500, JSON.stringify(err)))
})

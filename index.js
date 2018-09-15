const functions = require('firebase-functions')
const admin = require('firebase-admin')
const url = require('url')

admin.initializeApp(functions.config().firestore)
admin.firestore().settings({
  timestampsInSnapshots: true
})

exports.stop = functions.https.onRequest((req, res) => {
  const getRefForJob = jobId => {
    // TODO: refactor to "getRefForLastJob"
    return new Promise((resolve, reject) => {
      if (!jobId.length) {
        return reject(new Error('Please provide a valid job name.'))
      }
      admin
        .firestore()
        .collection('runs')
        .where('job', '==', jobId)
        .orderBy('start', 'desc')
        .limit(1)
        .get()
        .then(querySnapshot => {
          if (!querySnapshot || querySnapshot.empty) {
            return reject(new Error('Could not find this run!'))
          }
          const doc = querySnapshot.docs[0]
          if (doc.data().end !== null) {
            return reject(new Error('Already have an end time for this run!'))
          }
          return resolve(doc.ref)
        })
        .catch(err => reject(new Error(err)))
    })
  }

  const updateJobEndTime = jobRef => {
    return new Promise((resolve, reject) => {
      admin.firestore().runTransaction(t => {
        return t
          .get(jobRef)
          .then(doc => {
            const endTime = Date.now()
            const startTime = doc.data().start
            const duration = endTime - startTime
            t.update(jobRef, {
              duration: duration,
              end: Date.now()
            })
            return resolve('Job updated successfully.')
          })
          .catch(err => reject(new Error(err)))
      })
    })
  }

  getRefForJob(req.query.job)
    .then(jobRef => updateJobEndTime(jobRef))
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
  admin
    .firestore()
    .collection('runs')
    .where('job', '==', jobId)
    .orderBy('start', 'desc')
    .limit(1)
    .get()
    .then(querySnapshot => {
      if (querySnapshot && !querySnapshot.empty) { //TODO: refactor into promise
        // Make sure we haven't already ended this job.
        const doc = querySnapshot.docs[0]
        if (doc.data().end === null) {
          return res.send(500, "start before we ended this job????")
          // return reject(new Error('Already have an end time for this run!'))
        }
      }
      // Empty snapshot is fine, assume it's the first run.
      return admin
        .firestore()
        .collection('runs')
        .add({
          job: jobId,
          start: Date.now(),
          end: null
        })
        .then(snapshot => res.send(200))
        .catch(err => res.send(500, JSON.stringify(err)))
    }).catch(err => res.send(500, JSON.stringify(err)))
})

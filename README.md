# cronwatch "back-end" cloud functions

Right now the back-end consists entirely of Firebase cloud functions. This might be a horrible idea, but the current approach is to try a serverless architecture. If too many architectural sacrifices have to be made, this will be abandoned in favor of a more solid engineering approach.

Cronwatch cloud functions provide endpoints for jobs to signal starts, ends, and failures.

Additional features might include a job telemetry endpoint for a job, while running, to signal pertinent events in its lifecycle. This could be particularly useful for a long-running job that has multiple steps with variable lengths.

Ultimate big ideas would be a monitoring => controlling => validating solution (but first, we have to get good at monitoring). The ultimate goal is not to automate completely: rather, it's to remove the cognitive load from monitoring disparate and distributed processes and to provide relevant flags when intervention is required.

# But Why?

The general problem this code is trying to solve is:

1. We're trying to keep a server accountable to running on a schedule
2. To do this, we have a ledger of jobs it ought to run an times it ought to run them
3. How do we attribute jobs to the ledger and calculate variances from the ledger significant enough to warrant notification and/or interv

There are lots of products out there to do this, but sometimes it's fun to write your own. It'd also be cool if the project proves useful to others, either directly through code adoption or through some of the ideas it has or mistakes I make in implementing it.

## Frameworks and Modules

The functions use NodeJS with the [`moment`](https://www.npmjs.com/package/moment) module for date parsing and the [`cron-parser`](https://www.npmjs.com/package/cron-parser) module for parsing a cron signature.

Data is stored in Firebase.

## Data Structure

Currently using the Cloud Firestore to store data, a NoSQL database. So technically, it's a misnomer to talk about "primary/foreign keys." I chose this because it was free (ha!) and allows for rapid prototyping.

Two entities:

1. Jobs
2. Runs

### JOBS

jobs
* cronSig (e.g., `*/2 * * * *`)
* id (foreign key to tie to runs)

A job has many runs. A run has one job.

Jobs hold the job ID, which is used to tie a job to a run. Jobs also hold the `cronSig`. For example: 

```
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * command to execute
```
> ASCII copypasted directly from [Wikipedia article on cronjobs](https://en.wikipedia.org/wiki/Cron#Overview)


### RUNS

One run document per job run. Run documents are created *when they receive the run command*, not when a job **should** run. 

That is, if a job *ought* to run hourly but we don't receive any go commands, there will be 0 documents for the job that day, not 24 "blank" documents.

runs
* job => "primary key"
* duration (filled in when we receive the end command) TS
* start TS
* end TS
* error BOOL
* isValidStart BOOL
* wasStartValidated BOOL

All timestamps are stored in milliseconds since epoch UTC. All durations are in milliseconds.

## API

Jobs are identified by a `job=` param, which much be unique for each job.

A job with the given id must exit as a jobs document before a run can be generated based on that job. We'll still record the data, but we can't validate it, since we don't know what cron signature the job has.

At the start of a run, a job sends a get request to the go endpoint.

curl -sS 'https://us-central1-cronwatch-bcbd9.cloudfunctions.net/go?job=name'

At the end of a run, a job sends a get request to the stop endpoint.

curl -sS 'https://us-central1-cronwatch-bcbd9.cloudfunctions.net/stop?job=name'

If the job fails, it can send a failure status. This has the same effect as stop, but a fail flag is set.

curl -sS 'https://us-central1-cronwatch-bcbd9.cloudfunctions.net/fail?job=name'

For now, that's it.

Later, we might have a telemetry endpoint that sends "samples" periodically:

`curl -sS 'https://us-central1-cronwatch-bcbd9.cloudfunctions.net/sample?job=name&status=purringAlongMerrily'`

## Mechanics of Validation

This is likely to change as I'm still struggling with the best approach. It's best explained with an example.

### Lifecycle of a run

Take an example job that should run daily every 15 minutes. Its signature will be `*/15 * * * * *`. Let's call the job `purr`.

It's 0015. The server kicks off the cron job and sends *go* to the cloud function. A run document is created. A minute later the job finishes and sends stop to the cloud function.

How do we know which document to attribute the run to? We look for the last (most recently created) run document for the job. If that job has already received an end message, we fail -- this means there's no document to attribute the run to: either we never received a start for the run, we're getting a duplicate stop command, or something else is up.

If the last run's `endTime` is empty, however, we add the time and calculate and set the `duration`.

Then, we call `validateRunStarts()`. Whaaaaat?

#### Each time a run tells us it stopped, we validate all runs (that haven't been validated yet)

Going back to the "If too many architectural sacrifices have to be made..." caveat. `validateRunStarts` finds *all* jobs that have yet to be validated and runs a validation on them.

The assumption at play here: *all* of our server cronjobs aren't going to fail to run at the same time. When a job runs, it triggers validation of all the other jobs. Under normal circumstances, this will be just a couple of jobs (in testing, validating ~50 jobs tok at most 1 or 2 seconds).

The system is thus "self-sustaining" without the need for some external server to ping it periodically and tell it to run validations. The go command itself causes validations to be run. Clever things are often brittle, so whether this is a good idea remains to be seen.

### What is "validated"?

Each run has the `isValidStart` and `wasStartValidated` boolean flags. We consider a start to be "valid" if, given the cron signature, it occurred no sooner than 30 seconds and no later than 59 seconds than the job was scheduled to kick off.

The code for this:

```javascript
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
```

This looks really gross, but herre's the deal. For each job, fetch its cron signature. Parse the cron signature. `parser` returns an iteratore that can be queried for times of `.next()` and `.prev()` runs. Most of the time, the run we're validating is going to be immediately previous, *but not always*. I found it really slow to iterate over *all possible runs* from say the present to a day ago, especially if the run happens every 5 minutes.

`parser` allows us to set `currentDate` to whatever we want. So the strategy is: find a current date that we *know* will be *just after* the start of the run we're interested in, and then call `.prev()` to get the run time we expect to see.

I calculate the interval between two runs, then add subtract 30 seconds (the maximum allowed time a run can start early).

### Stuff this doesn't validate:

1. Runs that never started: I assume each run started *at some time* and that the only failure would be that it started really early or really late. This is a big deal, and should be fixed.

# To add

## Predictive Analytics

More sophisticated validation. For example, do some simple descriptive statistics on runs. Run time is expected to vary based on day of week, as well as during periods of heavy traffic. It's possible to normalize the variation by some constraint (hits per day, MB size of access logs, etc.) depending on the job. This is the purpose of maintaining a separate job document: metadata such as acceptable variance, validation types, alert levels, and alert methods can all be specified on a job-level. We might not want the same alert level for an "every five minutes health ping check" as we would for a "daily critical data ETL process." 

We can have job-level thresholds for error / warning, based on standard deviation of run times. If a job is +2 stddev of all time, something's wrong, or something's changed. Major changes in jobs might warrant a change in name (a big breaking change or change in scope of a job). Alternatively, metadata could be set for major job revisions on the job document and the validations processed differently over time, graded to the scope of the job.

## Notifications

### Integration with Slack / Email

The goals of this project is to make alerts that are:

1. Actionable
2. Accurate
3. Relevant
4. Severity-calibrated
5. Not redundant

I'm considering some sort of Slack integration for this. Basically, we want to avoid having a huge pile of alerts that are really just "info" or "warning" (using the log-level analogy). If something is informational, it should be visible on a dashboard or in the DB but not send us an e-mail or Slack message, unless perhaps "verbose" mode is enabled.

### Integration with BigQuery / Looker

Data can easily be exported from the Firestore into BigQuery at some pretedetermined interval to enable visualization with Looker. This means we'd technically never have to build a front-end. If the system is designed well enough, we'd never even need to look at the dashboard unless we're curious about run trends or looking for areas where we can optimize or increase/reduce server usage.

Ideally the monitor is a fire alarm that only goes off when something is indeed burning.

### Run hooks / dependency chains

Often jobs run in a pipeline, where subsequent jobs rely on previous jobs to create data that they then process. If jobs farther up the pipe fail, this can result in, at best, processing for no reason, and, at worst, corrupted data.

One potential add-on to the project (after it's proven to be reliable in alerting on jobs) is to make communciation between the monitor and the server bi-directional. While I think it would be foolish for the monitor to itself *trigger* jobs or trigger retries, we could define pipelines of tasks (chains of things that must occur) and have a job, on its `start` command, wait for an "OK" response from the server prior to proceeding.

### Monitor + control + validate

Since the monitor is integrated with BigQuery already and many of our tasks involve imports or other BigQuery validations, it would be fairly trivial to define an arbitrary number of validations, based on increase in table size / last modified date / etc for a given job.

The validation logic would be separate from the monitoring itself: I see this as 3 levels.

1. Monitor (make sure tasks run on time, alert if fails)
=> interfaces with
2. Controller (knows which tasks should happen when, and which tasks should be stopped if others fail to obtain)
=> makes decisions based on data obtained from
3. Validators
=> an interface to small jobs (perhaps independent cloud functions on a job-level) that implement custom logic to determine whethner things have run smoothly as the result of jobs

Steps #2 and #3 are far down the road, but since they would be quite useful, I want to design the monitor with extension to this level in mind.
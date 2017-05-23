package org.apache.kafka.connect.mongo

import org.slf4j.LoggerFactory
import org.apache.kafka.connect.mongo.MongoCronSourceConfig.Companion.SCHEDULE_CONFIG
import org.apache.kafka.connect.mongo.interfaces.AbstractMongoSourceTask
import org.quartz.*
import org.quartz.impl.StdSchedulerFactory

/**
 * @author Xu Jingxin
 */
class MongoCronSourceTask: AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoCronSourceTask::class.java)!!
    var schedule = ""
    var scheduler: Scheduler? = null

    override fun start(props: Map<String, String>) {
        log.info("Start schedule")
        super.start(props)
        schedule = props[SCHEDULE_CONFIG] ?: throw Exception("Invalid schedule!")
        startSchedule()
    }

    override fun stop() {
        log.info("Stop schedule")
        scheduler?.shutdown()
    }

    private fun startSchedule() {
        scheduler = StdSchedulerFactory.getDefaultScheduler()
        scheduler!!.start()
        databases.forEach { db ->
            val job = JobBuilder.newJob(CollectionExporter::class.java)
                    .setJobData(JobDataMap(mapOf(
                            "uri" to uri,
                            "db" to db,
                            "messages" to messages
                    )))
                    .withIdentity("job_mongo_exporter_$db", "group1")
                    .build()
            val trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger_mongo_exporter_$db", "group1")
                    .withSchedule(CronScheduleBuilder.cronSchedule(schedule))
                    .build()
            scheduler!!.scheduleJob(job, trigger)
        }
    }
}
